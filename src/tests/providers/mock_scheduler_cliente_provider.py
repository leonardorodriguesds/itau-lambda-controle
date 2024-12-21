class MockBotoExceptions:
    """Simula o namespace de exceções retornadas por boto3."""
    class ResourceNotFoundException(Exception):
        pass

class MockStepFunctionClient:
    """Mock do client 'stepfunctions' do boto3, armazenando execuções em memória."""

    def __init__(self):
        self._executions = {}  
        self.exceptions = MockBotoExceptions

    def start_execution(self, stateMachineArn, name, input):
        """
        Inicia uma execução em memória.
        """
        print(f"[MockStepFunctionClient] Starting execution: {name}")
        self._executions[name] = {
            "status": "RUNNING",
            "input": input,
            "stateMachineArn": stateMachineArn
        }
        return {"executionArn": f"arn:aws:states:::execution/{name}"}



class MockSchedulerClient:
    """Mock do client 'scheduler' do boto3, armazenando schedules em memória."""

    def __init__(self):
        self._schedules = {}  
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
        print(f"[MockSchedulerClient] Creating schedule: {Name}")
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
        return {"ScheduleArn": f"arn:aws:scheduler:::schedule/{Name}", "Success": True}

    def delete_schedule(self, Name):
        """
        Deleta um schedule, senão lança ResourceNotFoundException.
        """
        if Name not in self._schedules:
            raise self.exceptions.ResourceNotFoundException(f"Schedule '{Name}' not found.")

        del self._schedules[Name]
        return {"Success": True}



class MockSQSClient:
    """Mock do client 'sqs' do boto3, armazenando mensagens enviadas em memória."""

    def __init__(self):
        self._messages = []  
        self.exceptions = MockBotoExceptions

    def send_message(self, QueueUrl, MessageBody):
        message_id = f"msg-{len(self._messages) + 1}"
        print(f"[MockSQSClient] Sending message to {QueueUrl}, ID: {message_id}")
        self._messages.append({
            "QueueUrl": QueueUrl,
            "MessageBody": MessageBody,
            "MessageId": message_id
        })
        return {"MessageId": message_id}



class MockGlueClient:
    """Mock do client 'glue' do boto3, armazenando job runs em memória."""

    def __init__(self):
        self._job_runs = []  
        self.exceptions = MockBotoExceptions

    def start_job_run(self, JobName, Arguments):
        run_id = f"jr-{len(self._job_runs) + 1}"
        print(f"[MockGlueClient] Starting Glue Job: {JobName}, run_id: {run_id}")
        self._job_runs.append({
            "JobName": JobName,
            "Arguments": Arguments,
            "JobRunId": run_id
        })
        return {"JobRunId": run_id}



class MockLambdaClient:
    """Mock do client 'lambda' do boto3, armazenando invocações em memória."""

    def __init__(self):
        self._invocations = []  
        self.exceptions = MockBotoExceptions

    def invoke(self, FunctionName, InvocationType, Payload):
        request_id = f"req-{len(self._invocations) + 1}"
        print(f"[MockLambdaClient] Invoking Lambda: {FunctionName}, request_id: {request_id}")
        self._invocations.append({
            "FunctionName": FunctionName,
            "InvocationType": InvocationType,
            "Payload": Payload,
            "RequestId": request_id
        })
        return {"StatusCode": 202}



class MockEventsClient:
    """Mock do client 'events' do boto3, armazenando put_events em memória."""

    def __init__(self):
        self.exceptions = MockBotoExceptions
        self._put_events = []

    def put_events(self, Entries):
        """
        Armazena os eventos localmente.
        """
        print(f"[MockEventsClient] Putting {len(Entries)} event(s).")
        self._put_events.extend(Entries)
        return {
            "Entries": [
                {"EventId": f"evt-{i}"} for i, _ in enumerate(Entries)
            ]
        }


class MockResponse:
    def __init__(self, json_data, status_code=200):
        self._json = json_data
        self.status_code = status_code

    def json(self):
        return self._json

    def __getitem__(self, key):
        return self._json.get(key)

    def raise_for_status(self):
        pass  

class MockRequestsClient:
    """
    Mock de requests, armazenando requisições POST.
    Útil para simular o 'api_process'.
    """
    def __init__(self):
        self._posts = []

    def post(self, url, json):
        print(f"[MockRequestsClient] POST {url}")
        self._posts.append({"url": url, "json": json})
        return MockResponse({
            "ExecutionArn": "arn:aws:states:us-east-1:123456789012:execution:my-state-machine:fake-execution-id"
        })



from src.app.service.boto_service import BotoService

class MockBotoService(BotoService):
    """
    Substitui o BotoService real para testes.
    Retorna nossos mocks ao invés de um client real.
    """

    def __init__(
        self,
        mock_scheduler_client=None,
        step_function_client=None,
        sqs_client=None,
        glue_client=None,
        lambda_client=None,
        events_client=None,
        requests_client=None 
    ):
        self.scheduler = mock_scheduler_client or MockSchedulerClient()
        self.stepfunctions = step_function_client or MockStepFunctionClient()
        self.sqs = sqs_client or MockSQSClient()
        self.glue = glue_client or MockGlueClient()
        self.lambda_ = lambda_client or MockLambdaClient()
        self.events = events_client or MockEventsClient()
        self.requests = requests_client or MockRequestsClient()  

    def get_client(self, service_name: str):
        if service_name == 'scheduler':
            return self.scheduler
        elif service_name == 'stepfunctions':
            return self.stepfunctions
        elif service_name == 'sqs':
            return self.sqs
        elif service_name == 'glue':
            return self.glue
        elif service_name == 'lambda':
            return self.lambda_
        elif service_name == 'events':
            return self.events
        elif service_name == 'requests':
            return self.requests
        raise ValueError(f"MockBotoService não suporta o serviço: {service_name}")
