from functools import wraps
from logging import Logger
import os
import time
import traceback
from typing import Callable
from inspect import signature

from alembic.config import Config
from alembic import command

from aws_lambda_powertools.event_handler import ApiGatewayResolver
from aws_lambda_powertools.event_handler.exceptions import BadRequestError
from aws_lambda_powertools.utilities.typing import LambdaContext

from src.app.models.dto.task_executor_dto import TaskExecutorDTO
from src.app.models.dto.trigger_process_dto import TriggerProcess
from src.app.service.approval_status_service import ApprovalStatusService
from src.app.service.cloud_watch_service import CloudWatchService
from src.app.service.event_bridge_scheduler_service import EventBridgeSchedulerService
from src.app.service.task_executor_service import TaskExecutorService
from src.app.service.task_service import TaskService
from injector import Injector
from src.app.models.dto.table_dto import TableDTO
from src.app.models.dto.table_partition_exec_dto import TablePartitionExecDTO
from src.app.service.table_service import TableService
from src.app.service.table_partition_exec_service import TablePartitionExecService
from src.app.provider.session_provider import SessionProvider


class LambdaHandler:
    """
    Classe responsável por:
      1. Instanciar o ApiGatewayResolver (app).
      2. Conter os decorators (inject_dependencies, transactional, process_entities).
      3. Definir rotas organizadas por entidade.
      4. Expor um lambda_handler para ser usado na AWS Lambda.
    """

    def __init__(self, injector: Injector, app_resolver: ApiGatewayResolver):
        """
        :param injector: Instância do Injector para injeção de dependências.
        :param app_resolver: Instância do ApiGatewayResolver (ou qualquer outro roteador).
        """
        self.injector = injector
        self.cloudwatch_service = self.injector.get(CloudWatchService)
        self.logger = self.injector.get(Logger)
        self.app = app_resolver
        self.define_routes()

    def process_event(self, event, context: LambdaContext):
        """
        Lida com o evento do Lambda:
         - Loga métricas de início/fim
         - Chama o self.app (se for o caso) ou outra lógica de negócio
         - Retorna o response
        """
        start_time = time.time()
        error_count = 0
        route_called = event.get('path', 'unknown')

        try:
            self.logger.info(f"[{self.__class__.__name__}] Processing event on route: {route_called}")
            response = self.app.resolve(event, context)

        except Exception as e:
            stack_trace = traceback.format_exc()
            response = {
                "message": "Error processing event",
                "error": str(e),
                "stacktrace": stack_trace,
                "statusCode": 500
            }
            error_count += 1

        finally:
            total_execution_time = time.time() - start_time

            self.cloudwatch_service.add_metric(
                name="ExecutionTime",
                value=total_execution_time,
                unit="Milliseconds"
            )
            self.cloudwatch_service.add_metric(
                name="ErrorCount",
                value=error_count,
                unit="Count"
            )
            self.cloudwatch_service.add_metric(
                name="RouteCalled",
                value=1,
                unit="Count"
            )
            self.cloudwatch_service.flush_metrics()

        return response

    def inject_dependencies(self, func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            func_signature = signature(func)
            dependencies = {
                param_name: self.injector.get(param.annotation)
                for param_name, param in func_signature.parameters.items()
                if param.annotation is not param.empty and param_name not in kwargs
            }
            self.logger.debug(f"[{self.__class__.__name__}] Dependencies injected for {func.__name__}: {dependencies}")
            return func(*args, **dependencies, **kwargs)
        return wrapper

    def transactional(self, func: Callable):
        """
        Decorator para gerenciar o ciclo de vida da sessão:
        - Commit no sucesso
        - Rollback e log em caso de erro
        - Fechamento de sessão em todos os casos
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            session_provider: SessionProvider = kwargs.get("session_provider")
            if not session_provider:
                raise ValueError(
                    "`session_provider` é obrigatório para usar o decorator `@transactional`."
                )
            try:
                result = func(*args, **kwargs)
                session_provider.commit()
                return result
            except Exception as e:
                session_provider.rollback()
                logger = kwargs.get("logger")
                if logger:
                    logger.exception(f"[{self.__class__.__name__}] Erro na execução de {func.__name__}: {str(e)}")
                raise
            finally:
                session_provider.close()
        return wrapper

    def process_entities(self, func: Callable):
        """
        Annotation para processar múltiplos itens em `data`.
        Garante commit após cada item processado e rollback em caso de erro.
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            session_provider: SessionProvider = kwargs.get('session_provider')
            logger = kwargs.get('logger')

            body = self.app.current_event.json_body
            data = body.get("data")
            user = body.get("user")

            if not data:
                raise ValueError("Data is required")

            messages = []

            try:
                if isinstance(data, list):
                    for item in data:
                        try:
                            kwargs["entity_data"] = item
                            kwargs["user"] = user
                            message = func(*args, **kwargs)
                            messages.append(message)
                            session_provider.commit()
                            logger.debug(f"[{self.__class__.__name__}] Entity processed successfully: {item}")
                        except Exception as e:
                            session_provider.rollback()
                            logger.error(f"[{self.__class__.__name__}] Error processing entity: {item}. Error: {e}")
                            raise
                else:
                    kwargs["entity_data"] = data
                    kwargs["user"] = user
                    message = func(*args, **kwargs)
                    messages.append(message)
                    session_provider.commit()

                return {"message": "All entities processed successfully.", "details": messages}
            except Exception as e:
                session_provider.rollback()
                logger.exception(f"[{self.__class__.__name__}] Error processing entities: {e}")
                raise
            finally:
                session_provider.close()

        return wrapper

    def define_routes(self):
        """
        Método que registra as rotas organizadas por entidade.
        """
        self.define_migration_routes()
        self.define_approval_routes()
        self.define_table_routes()
        self.define_table_partition_exec_routes()  
        self.define_trigger_routes()
        self.define_task_executor_routes()
        self.define_health_route()

    def define_migration_routes(self):
        """
        Define as rotas relacionadas a migrações.
        """
        @self.app.post("/run_migrations")
        @self.inject_dependencies
        def run_migrations(logger: Logger):
            """
            Rota que executa migrações do Alembic mediante uma palavra-chave no body.
            """
            body = self.app.current_event.json_body
            keyword = body.get("keyword")

            if keyword != os.getenv("MIGRATION_KEYWORD", "RUN_MIGRATIONS"):
                logger.warning("Tentativa de executar migrações sem keyword válida.")
                return {
                    "message": "Invalid or missing keyword. No migrations were run."
                }

            logger.info("Rodando migrações Alembic, pois keyword foi válida.")
            alembic_cfg = Config("alembic.ini")
            command.upgrade(alembic_cfg, "head")
            logger.info("Migrações Alembic executadas com sucesso.")

            return {
                "message": "Migrations ran successfully."
            }

    def define_approval_routes(self):
        """
        Define as rotas relacionadas a aprovação de tarefas.
        """
        @self.app.post("/approve")
        @self.inject_dependencies
        @self.transactional
        def approve_task(
            approval_status_service: ApprovalStatusService,
            event_bridge_schedule_service: EventBridgeSchedulerService,
            session_provider: SessionProvider,
            logger: Logger
        ):
            body = self.app.current_event.json_body
            approval_status_id = body.get("approval_status_id")
            user = body.get("user")

            if not approval_status_id:
                raise BadRequestError("approval_status_id is required")

            approval_status = approval_status_service.approve(approval_status_id, user)

            event_bridge_schedule_service.schedule(approval_status.task_schedule)

            logger.info("Event processed successfully.")
            return {"message": "Task approved successfully."}

        @self.app.post("/reject")
        @self.inject_dependencies
        @self.transactional
        def reject_task(
            approval_status_service: ApprovalStatusService,
            session_provider: SessionProvider,
            logger: Logger
        ):
            body = self.app.current_event.json_body
            approval_status_id = body.get("approval_status_id")
            user = body.get("user")

            if not approval_status_id:
                raise BadRequestError("approval_status_id is required")

            approval_status_service.reject(approval_status_id, user)

            logger.info("Event processed successfully.")
            return {"message": "Task rejected successfully."}

    def define_table_routes(self):
        """
        Define as rotas relacionadas a tabelas.
        """
        @self.app.route("/tables", method="POST")
        @self.inject_dependencies
        @self.process_entities
        def create_table(
            table_service: TableService,
            entity_data: dict,
            user: str,
            session_provider: SessionProvider,
            logger: Logger
        ):
            """
            Rota para criar uma nova tabela.
            """
            table_dto = TableDTO(**entity_data)

            if table_dto.id:
                raise BadRequestError("ID não deve ser fornecido para criação de tabela")

            message = table_service.save_table(table_dto, user)
            logger.info(f"Tabela criada com sucesso: {message}")
            return message, 201  

        @self.app.put("/tables/<table_id>", summary="Atualizar uma tabela existente", tags=["Tables"])
        @self.inject_dependencies
        @self.process_entities
        def update_table(
            table_id: int,
            table_service: TableService,
            entity_data: dict,
            user: str,
            session_provider: SessionProvider,
            logger: Logger
        ):
            """
            Rota para atualizar uma tabela existente.
            """
            if not table_id:
                raise BadRequestError("Table ID is required for update")
            
            table_dto = TableDTO(**entity_data)
            table_dto.id = table_id

            message = table_service.save_table(table_dto, user)
            logger.info(f"Tabela atualizada com sucesso: {message}")
            return message, 200  
        
        @self.app.delete("/tables/<table_id>", summary="Excluir uma tabela por ID", tags=["Tables"])
        @self.inject_dependencies
        @self.transactional
        def delete_table(
            table_id: int,
            table_service: TableService,
            session_provider: SessionProvider,
            logger: Logger
        ):
            """
            Rota para excluir uma tabela específica pelo seu ID.
            """
            if not table_id:
                raise BadRequestError("Table ID is required for deletion")

            logger.info(f"Attempting to delete table with ID: {table_id}")
            table_service.delete(table_id)
            logger.info(f"Table with ID: {table_id} deleted successfully.")
            return {"message": f"Table with ID {table_id} deleted successfully."}

        @self.app.get("/tables")
        @self.inject_dependencies
        def get_tables(
            table_service: TableService,
            logger: Logger
        ):
            logger.debug(f"[{self.__class__.__name__}] Getting tables: {self.app.current_event.query_string_parameters}")
            filters = self.app.current_event.query_string_parameters
            tables = table_service.query(**filters)
            logger.debug(f"[{self.__class__.__name__}] Tables found: {tables}")
            return [table.json_dict() for table in tables]

    def define_table_partition_exec_routes(self):
        """
        Define as rotas relacionadas à execução de partições de tabelas.
        """
        @self.app.post("/register_execution")
        @self.inject_dependencies
        @self.process_entities
        def register_execution(
            table_partition_exec_service: TablePartitionExecService,
            entity_data: dict,
            user: str,
            session_provider: SessionProvider,
            logger: Logger
        ):
            """
            Rota para registrar a execução de partições de tabelas.
            """
            execution_dto = TablePartitionExecDTO(**entity_data, user=user)
            message = table_partition_exec_service.register_partitions_exec(execution_dto)
            logger.info("Execution registered successfully.")
            return {"message": "Execution registered successfully."}

    def define_trigger_routes(self):
        """
        Define as rotas relacionadas a triggers e processos.
        """
        @self.app.post("/trigger")
        @self.inject_dependencies
        @self.transactional
        def trigger_event(
            task_service: TaskService,
            logger: Logger,
            session_provider: SessionProvider
        ):
            payload = self.app.current_event.json_body

            if not payload:
                raise BadRequestError("Payload is required")

            if not payload.get("task_table"):
                raise BadRequestError("Task table is required in payload")

            if not payload.get("execution"):
                raise BadRequestError("Execution is required in payload")

            if not payload.get("task_schedule"):
                raise BadRequestError("Task schedule is required in payload")

            task_table_id = payload["task_table"]["id"]
            dependency_execution_id = payload["execution"]["id"]
            task_schedule_id = payload["task_schedule"]["id"]

            logger.debug(f"Received trigger request: {payload}")
            logger.info(f"Processing trigger for task table ID: {task_table_id}")

            task_service.trigger_tables(
                task_schedule_id=task_schedule_id,
                task_table_id=task_table_id,
                dependency_execution_id=dependency_execution_id
            )

            logger.info(f"Trigger processed successfully for task table ID: {task_table_id}")
            return {"message": "Trigger processed successfully."}

        @self.app.post("/run")
        @self.inject_dependencies
        @self.transactional
        def trigger_process(
            task_service: TaskService,
            logger: Logger,
            session_provider: SessionProvider
        ):
            body = self.app.current_event.json_body
            payload = TriggerProcess(**body)

            if not payload:
                raise BadRequestError("Payload is required")

            if not payload.table_id and not payload.table_name:
                raise BadRequestError("Table ID or name is required in payload")

            if not payload.task_id and not payload.task_name:
                raise BadRequestError("Task ID or name is required in payload")

            task_service.run(payload)

            logger.info("Event processed successfully.")
            return {"message": "Task processed successfully."}

    def define_task_executor_routes(self):
        """
        Define as rotas relacionadas a task executors.
        """
        @self.app.post("/task_executor")
        @self.inject_dependencies
        @self.transactional
        def create_task_executor(
            task_executor_service: TaskExecutorService,
            logger: Logger,
            session_provider: SessionProvider
        ):
            body = self.app.current_event.json_body
            payload = TaskExecutorDTO(**body)

            if not payload:
                raise BadRequestError("Payload is required")

            task_executor_service.save(payload)

            logger.info("Event processed successfully.")
            return {"message": "Task executor created successfully."}

        @self.app.delete("/task_executor/<task_executor_id>", summary="Excluir uma tarefa por ID", tags=["Task Executor"])
        @self.inject_dependencies
        @self.transactional
        def delete_task_executor(
            task_executor_id: int,
            task_executor_service: TaskExecutorService,
            logger: Logger,
            session_provider: SessionProvider
        ):
            if not task_executor_id:
                raise BadRequestError("Task Executor ID is required")
            task_executor_service.delete(task_executor_id)

            logger.info("Event processed successfully.")
            return {"message": "Task executor deleted successfully."}

    def define_health_route(self):
        """
        Define a rota de health check.
        """
        @self.app.get("/health")
        @self.inject_dependencies
        def health_check(logger: Logger):
            logger.info("Health check route accessed")
            return {"status": "OK"}