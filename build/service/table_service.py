from models.tables import Tables
from models.task_executor import TaskExecutor
from models.dependencies import Dependencies
from models.partitions import Partitions
from service.database import get_session
from app.exceptions.table_insert_error import TableInsertError
from app.models.dto.table_dto import TableDTO, PartitionDTO, DependencyDTO, TaskDTO

def save(table_dto: TableDTO, user: str):
    if not table_dto:
        return "Data parameter is required for save"

    session_generator = get_session()
    session = next(session_generator)
    
    try:
        task_executor_id = None
        if table_dto.tasks:
            for task in table_dto.tasks:
                executor = session.query(TaskExecutor).filter(TaskExecutor.id == task.task_executor_id).first()
                if not executor:
                    executor = TaskExecutor(alias=task.alias, description=task.params.get("description", ""))
                    session.add(executor)
                    session.commit()
                task_executor_id = executor.id

        if table_dto.id:
            table = session.query(Tables).filter(Tables.id == table_dto.id).first()
            if not table:
                return f"Table with id {table_dto.id} not found."

            table.name = table_dto.name
            table.description = table_dto.description
            table.requires_approval = table_dto.requires_approval
            table.last_modified_by = user
            message = f"Table with id {table.id} updated successfully."
        else:
            table = Tables(
                name=table_dto.name,
                description=table_dto.description,
                requires_approval=table_dto.requires_approval,
                created_by=user,
                last_modified_by=user
            )
            session.add(table)
            session.commit()
            message = f"Table added successfully with table_id: {table.id}"

        if table_dto.partitions:
            existing_partitions = {p.name for p in session.query(Partitions).filter(Partitions.table_id == table.id).all()}
            for partition_data in table_dto.partitions:
                if partition_data.name not in existing_partitions:
                    partition = Partitions(
                        table_id=table.id,
                        name=partition_data.name,
                        type=partition_data.type,
                        is_required=partition_data.is_required
                    )
                    session.add(partition)

        if table_dto.dependencies:
            existing_dependencies = {d.dependency_id for d in session.query(Dependencies).filter(Dependencies.table_id == table.id).all()}
            for dependency_data in table_dto.dependencies:
                dependency_id = dependency_data.dependency_id
                if dependency_id and dependency_id not in existing_dependencies:
                    dependency = Dependencies(table_id=table.id, dependency_id=dependency_id)
                    session.add(dependency)
                elif not dependency_id:
                    new_dependency_table = Tables(
                        name=dependency_data.dependency_name,
                        description=dependency_data.dependency_description,
                        requires_approval=dependency_data.requires_approval,
                        created_by=user,
                        last_modified_by=user
                    )
                    session.add(new_dependency_table)
                    session.commit()  

                    dependency = Dependencies(table_id=table.id, dependency_id=new_dependency_table.id)
                    session.add(dependency)

        session.commit()
        
        return message

    except Exception as e:
        session.rollback()
        raise TableInsertError(f"Error saving table: {str(e)}")

    finally:
        session_generator.close()