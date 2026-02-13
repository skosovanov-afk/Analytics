# Установка Python 3.12 для проекта

## Вариант 1: Официальный установщик (рекомендуется)

1. Скачайте установщик Python 3.12.12 с официального сайта:
   https://www.python.org/downloads/macos/

2. Запустите установщик и следуйте инструкциям

3. После установки создайте виртуальное окружение:
   ```bash
   python3.12 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

## Вариант 2: Через Homebrew

Если у вас установлен Homebrew:
```bash
brew install python@3.12
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Вариант 3: Через pyenv (для управления версиями)

```bash
# Установка pyenv через Homebrew
brew install pyenv

# Добавьте в ~/.zshrc:
echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.zshrc
echo 'command -v pyenv >/dev/null || export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.zshrc
echo 'eval "$(pyenv init -)"' >> ~/.zshrc

# Перезагрузите shell или выполните:
source ~/.zshrc

# Установите Python 3.12
pyenv install 3.12.12
pyenv local 3.12.12

# Создайте виртуальное окружение
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## После установки

Запустите приложение:
```bash
source .venv/bin/activate
uvicorn app.main:app --reload
```

Откройте в браузере: http://127.0.0.1:8000/
