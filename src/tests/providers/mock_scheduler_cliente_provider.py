from src.app.service.boto_service import BotoService

class MockBotoExceptions:
    """Simula o namespace de exceções retornadas por boto3."""
    class ResourceNotFoundException(Exception):
        pass
    
class MockStepFunctionClient:
    """Mock do client 'stepfunctions' do boto3, armazenando execuções em memória."""
    
    def __init__(self):
        self._executions = {}  # dict { execution_arn: { "status": ..., "input": ... } }
        self.exceptions = MockBotoExceptions
    
    def start_execution(self, stateMachineArn, name, input):
        """
        Inicia uma execução em memória.
        """
        print(f"Starting execution: {name}")
        self._executions[name] = {
            "status": "RUNNING",
            "input": input,
            "stateMachineArn": stateMachineArn
        }
        return {"executionArn": f"arn:aws:states:::execution/{stateMachineArn}"}

class MockSchedulerClient:
    """Mock do client 'scheduler' do boto3, armazenando schedules em memória."""

    def __init__(self):
        self._schedules = {}  # dict { schedule_name: { "ScheduleExpression": ..., "Target": ... } }
        self.exceptions = MockBotoExceptions

    def list_schedules(self, NamePrefix=None):
        """
        Retorna schedules cujos nomes comecem com NamePrefix (ignora case).
        """
        schedules = []
        for name, schedule_info in self._schedules.items():
            if NamePrefix and name.lower().startswith(NamePrefix.lower()):
                schedules.append({"Name": name, **schedule_info})
        return {"Schedules": schedules}

    def create_schedule(self, Name, ScheduleExpression, FlexibleTimeWindow, Target):
        """
        Cria um 'Schedule' em memória.
        """
        print(f"Creating schedule: {Name}")
        self._schedules[Name] = {
            "ScheduleExpression": ScheduleExpression,
            "FlexibleTimeWindow": FlexibleTimeWindow,
            "Target": Target
        }
        return {"ScheduleArn": f"arn:aws:scheduler:::schedule/{Name}"}

    def update_schedule(self, Name, ScheduleExpression, FlexibleTimeWindow, Target):
        """
        Atualiza um schedule se existir, senão lança ResourceNotFoundException.
        """
        if Name not in self._schedules:
            raise self.exceptions.ResourceNotFoundException(f"Schedule '{Name}' not found.")

        self._schedules[Name].update({
            "ScheduleExpression": ScheduleExpression,
            "FlexibleTimeWindow": FlexibleTimeWindow,
            "Target": Target
        })
        return {
            "ScheduleArn": f"arn:aws:scheduler:::schedule/{Name}",
            "Success": True
        }

    def delete_schedule(self, Name):
        """
        Deleta um schedule, senão lança ResourceNotFoundException.
        """
        if Name not in self._schedules:
            raise self.exceptions.ResourceNotFoundException(f"Schedule '{Name}' not found.")

        del self._schedules[Name]
        return {"Success": True}


class MockBotoService(BotoService):
    """
    Substitui o BotoService real para testes,
    retornando um MockSchedulerClient ao invés de um client real.
    """

    def __init__(self, mock_scheduler_client=None, step_function_client=None):
        self.scheduler = mock_scheduler_client if mock_scheduler_client else MockSchedulerClient()
        self.stepfunctions = step_function_client if step_function_client else MockStepFunctionClient()

    def get_client(self, service_name: str):
        if service_name == 'scheduler':
            return self.scheduler
        elif service_name == 'stepfunctions':
            return self.stepfunctions
        raise ValueError(f"MockBotoService não suporta o serviço: {service_name}")
