import sqlite3
from datetime import datetime
import argparse

config_dir = "conf.txt"

def output(message):
  print(message)

def read_config_file(filename):
  try:
    with open(filename, 'r') as file:
      for line in file:
        key, value = line.strip().split('=')
        if key.strip() == 'files_dir':
          files_dir = value.strip()
        elif key.strip() == 'ext':
          ext = value.strip()
        elif key.strip() == 'format':
          log_format = value.strip()
    return files_dir, ext, log_format
  except FileNotFoundError:
    output("Настроечный файл не найден.")
    return None, None, None
  except UnboundLocalError:
    output("В настройках файла не хватает данных.")
    return None, None, None

def read_logs_file(filename):
  try:
    with open(filename, 'r') as file:
      logs_information = [line.split() for line in file]
    return logs_information
  except FileNotFoundError:
    output("Файл с логами не найден.")

def check_db():
  conn = sqlite3.connect('database.db')
  cursor = conn.cursor()
  cursor.execute('''
  CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      password TEXT NOT NULL,
      tabelID TEXT NOT NULL
  )
  ''')
  return conn, cursor

def format_log(log, filter_str):
  ip, ident, user, raw_time, request, status, size = log
  time = raw_time[1:]
  request = request[1:-1]

  replacements = {
    '%h': ip,
    '%l': ident if ident != '-' else '-',
    '%u': user if user != '-' else '-',
    '%t': f"[{time}",
    '%r': request,
    '%>s': status,
    '%b': str(size)
  }

  for key, value in replacements.items():
    filter_str = filter_str.replace(key, value)

  return filter_str

def parse_log_date(log_date_str):
  log_date_str = log_date_str[1:].split()[0]
  return datetime.strptime(log_date_str, '%d/%b/%Y:%H:%M:%S')

def filter_logs_by_date(logs, date1, date2=None):
  datetime1 = parse_log_date(date1).time
  datetime2 = parse_log_date(date2) if date2 else datetime1
  return [log for log in logs if datetime1 <= parse_log_date(log[2]) <= datetime2]

def user_exists(name, cursor):
  cursor.execute("SELECT 1 FROM users WHERE name = ?", (name,))
  return cursor.fetchone() is not None

class User:
  def __init__(self, conn, cursor):
    self.conn = conn
    self.cursor = cursor
    self.name = ""
    self.tabelID = ""

  def create_user(self):
    name = input("Введите логин: ")
    password = input("Введите пароль: ")
    confirm_password = input("Подтвердите пароль: ")
    if password != confirm_password:
      output("Пароли разные!")
      return None
    tabelID = f"{name}_id"
    self.cursor.execute("INSERT INTO users (name, password, tabelID) VALUES (?, ?, ?)", (name, password, tabelID))
    self.conn.commit()
    output(f"Аккаунт {name} успешно создан.")
    return tabelID

  def login(self):
    self.load_user_from_file()
    if not self.name:
      while True:
        name = input("Введите логин: ")
        if user_exists(name, self.cursor):
          password = input("Введите пароль: ")
          self.cursor.execute("SELECT * FROM users WHERE name = ? AND password = ?", (name, password))
          user = self.cursor.fetchone()
          if user:
            self.name = name
            self.tabelID = user[3]
            break
          else:
            output("Неверный пароль.")
        else:
          output("Аккаунт не обнаружен.")
          if input("Создать новый? да/нет: ").lower() == "да":
            self.tabelID = self.create_user()
    self.save_user_to_file()
    output(f"Добро пожаловать, {self.name}!")

  def save_user_to_file(self, filename="user_data.txt"):
    if self.name and self.tabelID:
      with open(filename, "w") as file:
        file.write(f"{self.name},{self.tabelID}\n")

  def load_user_from_file(self, filename="user_data.txt"):
    try:
      with open(filename, "r") as file:
        line = file.readline().strip()
        if line:
          self.name, self.tabelID = line.split(',')
          self.tabelID = self.tabelID
          output(f"Данные пользователя загружены: {self.name}, ID: {self.tabelID}")
    except FileNotFoundError:
      pass

  def save_information(self, logs):
    self.cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {self.tabelID} (
            hostname TEXT,
            remote_logname TEXT,
            remote_user TEXT,
            time TEXT,
            first_line TEXT,
            status TEXT,
            size_of_response_in_bytes INTEGER
        )
    ''')
    for log in logs:
      if len(log) == 9:
        first_line = f"{log[4]} {log[5]} {log[6]}"
        formatted_log = (log[0], log[1], log[2], log[3], first_line, log[7], log[8])
        self.cursor.execute(f'''
          INSERT INTO {self.tabelID} (hostname, remote_logname, remote_user, time, first_line, status, size_of_response_in_bytes)
          VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', formatted_log)
      else:
        output(f"Некорректное кол-во элементов в логе: {log}, {len(log)}")
    self.conn.commit()

  def read_information(self):
    try:
      self.cursor.execute(f"SELECT * FROM {self.tabelID}")
      return self.cursor.fetchall()
    except sqlite3.Error as e:
      output(f"Ошибка при чтении данных из базы данных: {e}")
      return None

def output_d(date_str1=None, date_str2=None):
  files_dir, ext, log_format = read_config_file(config_dir)
  logs = user.read_information()
  if date_str1 and date_str2:
    date1 = datetime.strptime(date_str1, '%Y-%m-%d').date()
    date2 = datetime.strptime(date_str2, '%Y-%m-%d').date()
    filtered_logs = filter_logs_by_date(logs, date1, date2)
  elif date_str1:
    date1 = datetime.strptime(date_str1, '%Y-%m-%d').date()
    filtered_logs = filter_logs_by_date(logs, date1)
  else:
    filtered_logs = logs

  for log in filtered_logs:
    print(format_log(log, log_format))

def parse():
  files_dir, ext, log_format = read_config_file(config_dir)
  user.save_information(read_logs_file(files_dir))

def leave():
  with open("user_data.txt", 'w') as file:
    file.write('')

def reader():
  while True:
    func = input('Выберите функцию leave, parse, output ')
    if func == "leave":
      leave()
      main()
      break
    elif func == "parse":
      parse()
    elif func == "output":
      date1 = input("Введите первую дату в формате 00/jan/0000:00:00:00 или нажмите Enter: ")
      date2 = input("Введите вторую дату в формате 00/jan/0000:00:00:00 или нажмите Enter: ")
      if not date1 and not date2:
        output_d()
      else:
        try:
          date1 = parse_log_date(date1)
          date2 = parse_log_date(date2)
          output_d(date1, date2)
        except ValueError:
          output("Неверный формат даты!")
    else:
      output("Такой функции не существует!")

def main():
  global user
  global files_dir, ext, log_format

  files_dir, ext, log_format = read_config_file(config_dir)
  conn, cursor = check_db()
  user = User(conn, cursor)
  user.login()

  reader()
  input()
  conn.close()

if __name__ == "__main__":
  main()
