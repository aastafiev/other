<b>Unix:</b><br>
- export PYTHONPATH="$PYTHONPATH:/path_to_myapp/myapp/myapp/"

<b>Windows:</b><br>
- set PYTHONPATH=%PYTHONPATH%;D:\LocalData\au00681\PycharmProjects\other

<b>Update requirements:</b><br>
- добавить название пакета в `requirements.in`
- выполнить команду `pip-compile --output-file requirements.txt requirements.in`
для автоматического обновления файла `requirements.txt`
- выполнить `pip-sync` для установки новых зависимостей

<b>Install requirements:</b><br>
- pip install -r requirements.txt