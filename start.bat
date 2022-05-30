@echo off

@echo Please wait...
cmd /k "python -m venv venv & cd venv\Scripts & activate.bat & pip install -r ../../requirements.txt & cd ../../scrapper & python scrapper.py & exit"