from setuptools import setup, find_packages
from itaufluxcontrol.versioncontrol import version_control

setup(
    name='itaufluxcontrol',
    version=version_control['version'],
    description='Controle de fluxo Itaú',
    author='Leonardo Rodrigues',
    author_email='leonardo.a.rodrigues@itau-unibanco.com.br',
    url='',  
    packages=find_packages(where=".", exclude=("tests*", "docs*")),
    include_package_data=True,
    install_requires=[
        "sqlalchemy>=2.0.36",
        "alembic>=1.14"
    ],
    extras_require={
        "dev": ["pytest", "flake8", "black"],
    },
    entry_points={},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.11',
)
