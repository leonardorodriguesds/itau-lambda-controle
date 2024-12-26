class TableInsertError(Exception):
    """Exceção personalizada para erros de inserção na tabela Tables."""
    
    def __init__(self, message="Failed to insert table into the database"):
        self.message = message
        super().__init__(self.message)
    
    def __str__(self):
        return f"TableInsertError: {self.message}"
