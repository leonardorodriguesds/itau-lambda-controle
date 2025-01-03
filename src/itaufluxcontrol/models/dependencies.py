from sqlalchemy import Boolean, Column, Integer, ForeignKey
from sqlalchemy.orm import relationship
from .base import AbstractBase

class Dependencies(AbstractBase):
    __tablename__ = 'dependencies'
    
    id = Column(Integer, primary_key=True)
    table_id = Column(Integer, ForeignKey('tables.id'), nullable=False)
    dependency_id = Column(Integer, ForeignKey('tables.id'), nullable=False)
    optative_with_dependency_id = Column(Integer, ForeignKey('dependencies.id'), nullable=True)
    is_required = Column(Boolean, default=False)
    
    table = relationship("Tables", foreign_keys=[table_id], back_populates="dependencies")
    dependency_table = relationship("Tables", foreign_keys=[dependency_id], back_populates="dependent_tables")
    optative_with_dependency = relationship("Dependencies", foreign_keys=[optative_with_dependency_id])
