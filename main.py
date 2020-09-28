from contextlib import contextmanager
from dataclasses import dataclass, InitVar
from datetime import date, timedelta
import re
from functools import reduce
from typing import List

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, String, Date
from sqlalchemy.orm import sessionmaker


# магия регэксп не нужно в этой задаче
regex = "^[a-z0-9]+[\._]?[a-z0-9]+[@]\w+[.]\w{2,3}$"
"""
^[a-z0-9]+ в начале шаблона одно или более вхождений символов a-z0-9
[\._]? ноль или одно вхождение \._  валидны адреса почт вида ааа_ddd@gmail.com
[a-z0-9]+ одно или более вхождений символов a-z0-9
[@]
\w+ одно или более вхождений любых букв (то, что может быть частью слова), а также цифры и _ (домен почтовика)
[.]
\w{2,3}$ в конце шаблона 2 или 3 вхождений любых букв (то, что может быть частью слова), а также цифры и _ 
"""


# путь к базе
DB_PATH = "sqlite:///sochi_athletes.sqlite3"

# ORM модель двух баз: атлетов и юзеров
Base = declarative_base()


class Athelete(Base):
    __tablename__ = "athelete"

    id = Column(Integer, primary_key=True)
    age = Column(Integer)
    birthdate = Column(String)
    gender = Column(String)
    height = Column(Float)
    name = Column(String)
    weight = Column(Integer)
    gold_medals = Column(Integer)
    silver_medals = Column(Integer)
    total_medals = Column(Integer)
    sport = Column(String)
    country = Column(String)

    def __repr__(self):
        return (
            "<Athelete(age='%s', birthdate='%s', gender='%s',"
            "height='%s', name='%s', weight='%s', gold_medals='%s',"
            "silver_medals='%s', total_medals='%s', sport='%s', country='%s')>"
            % (
                self.age,
                self.birthdate,
                self.gender,
                self.height,
                self.name,
                self.weight,
                self.gold_medals,
                self.silver_medals,
                self.total_medals,
                self.sport,
                self.country,
            )
        )


class User(Base):
    __tablename__ = "user"

    id = Column(Integer, primary_key=True)
    first_name = Column(String)
    last_name = Column(String)
    gender = Column(String)
    email = Column(String)
    birthdate = Column(Date)  # <class 'datetime.date'>
    height = Column(Float)

    def __repr__(self):
        return "<User(first_name='%s', last_name='%s', gender='%s', email='%s', birthdate='%s', height='%s',)>" % (
            self.first_name,
            self.last_name,
            self.gender,
            self.email,
            self.birthdate,
            self.height,
        )
   
    @classmethod
    def make_record(cls, arg):
        """
        метод класса для создания записи в базу из датакласса UserInput
        """
        return cls(
            first_name=arg.first_name,
            last_name=arg.last_name,
            gender=arg.gender,
            email=arg.email,
            birthdate=arg.birthdate,
            height=arg.height,
        )

# коннект к базе
eng = create_engine(DB_PATH)

# таблицы
# print(eng.table_names())

# создание конструктора сессий
Session = sessionmaker(bind=eng)


# управление сессиями
@contextmanager
def session_context():
    # созданеи экземляра сессии
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise Exception("Ошибка записи в базу")
    finally:
        session.close()


class ManagerDb:
    """
    общий доступ к базе данных
    """

    def __init__(self, orm_class: Base, s: Session):
        self.orm_class = orm_class
        self.s = s

    def __iter__(self):
        return iter(self.s.query(self.orm_class).all())

    def __len__(self):
        return len(self.s.query(self.orm_class).all())

    @property
    def query(self):
        return self.s.query(self.orm_class)


class UserDb(ManagerDb):
    def __init__(self, s: Session):
        super().__init__(User, s)

    def append(self, orm_class):
        self.s.add(orm_class)

    def query_id(self, first_name, last_name):
        q = self.query.filter(
            self.orm_class.first_name == first_name,
            self.orm_class.last_name == last_name,
        )
        if self.s.query(q.exists()).scalar():
            return q.one()
        else:
            return None


class AtheleteDb(ManagerDb):
    def __init__(self, s):
        super().__init__(Athelete, s)

    def nearest_height(self, height):
        # отфильтруем атлетов у кого не указан рост
        items = filter(lambda x: x.height, [i for i in self])
        # берем два значения из списка, вычитаем рост (abs абсолютное значение без знака) если разность меньше чем у предыдущего выкидываем
        return reduce(
            lambda x, y: y if abs(y.height - height) < abs(x.height - height) else x,
            items,
        )

    def nearest_birthdate(self, birthdate):
        # отфильтруем атлетов у кого не указана birthdate
        items = filter(lambda x: x.birthdate, [i for i in self])
        # сортируем список дней рождения от более старых к более молодым атлетам
        # берем два дня рождения из списка, у кого раньше того выкидываем
        return reduce(
            lambda x, y: y if date.fromisoformat(x.birthdate) < birthdate else x,
            sorted(items, key=lambda x: x.birthdate),
        )


# разбор ввода пользователя:


def age_calculation(birthdate: date) -> int:
    """
    вычисление полных лет пользователя.
    вообще не нужно в этой задаче
    """
    today = date.today()  # дата сегодня
    years = today.year - birthdate.year
    # если месяц  рождения еще не наступил то вычитаем 1 год
    # если месяц рождения наступил но день еще нет тоже вычитаем
    if today.month < birthdate.month or (
        today.month == birthdate.month and today.day < birthdate.day
    ):
        years -= 1
    return years


def check_email(email: str) -> bool:
    """
    валидация емайл
    вообще не нужно в этой задаче
    """
    if re.match(regex, email):
        return True
    else:
        return False


@dataclass
class UserInput:
    """
    парсер ввода пользователя
    вообще не нужно в этой задаче
    """

    first_name: str
    last_name: str
    gender: str
    email: str
    height: int
    birthdate: date = None
    age: int = None
    # значение birthdate_raw только для вычисления birthdate и age
    birthdate_raw: InitVar[str] = None

    def __post_init__(self, birthdate_raw):
        # парсинг даты из строки исо дата
        if self.birthdate is None and birthdate_raw is not None:
            try:
                self.birthdate = date.fromisoformat(birthdate_raw)
            except ValueError as e:
                print(e)
                raise ValueError(
                    "Дата рождения пользователя должна быть в формате ISO YYYY-MM-DD"
                )
            # вычисление полных лет
            self.age = age_calculation(self.birthdate)
        # пример валидации ввода пользователя
        if not (self.gender == "Male" or self.gender == "Female"):
            raise ValueError('Значение пола юзера только "Male" или "Female"')
        if not check_email(self.email):
            raise ValueError("Не корректный емайл")
        if self.height <= 0 or self.height > 2.50:
            raise ValueError("Значение роста юзера должна больше нуля и меньше 2.50 м")

    @classmethod
    def make_user_input(cls, arg):
        """
        класс метод создания UserInput из args argparse
        """
        return cls(
            args.first_name,
            args.last_name,
            args.gender,
            args.email,
            args.height,
            birthdate_raw=args.birthdate,
        )


# main функции:


def add(args):
    """
    добавление пользователя
    """
    user = UserInput.make_user_input(args)
    record = User.make_record(user)
    with session_context() as session:
        UserDb(session).append(record)
        session.flush()
        print(f"Запись добавлена. ID = {record.id}")


def query_id(args):
    """
    запрос id пользователя
    """
    birthdate, height = None, None
    with session_context() as session:
        user = UserDb(session).query_id(args.first_name, args.last_name)
        if user:
            print(f"id пользователя - {user.id}")
            birthdate = user.birthdate
            height = user.height
            athelete_height = AtheleteDb(session).nearest_height(height)
            print(
                f"Ближайший по росту атлет {athelete_height.name}. Рост {athelete_height.height}"
            )
            athelete_birthdate = AtheleteDb(session).nearest_birthdate(birthdate)
            print(
                f"Ближайший по возрасту атлет {athelete_birthdate.name}. День рождения {athelete_birthdate.birthdate}"
            )
        else:
            print(f"Пользователя {args.first_name} {args.last_name} не найдено")


def show_users(args):
    # не получилось избавится от args
    with session_context() as session:
        for item in UserDb(session):
            print(item)


def query_athelete(args):
    """
    ответы на вопросы
    """
    with session_context() as session:
        q1 = AtheleteDb(session).query.filter(Athelete.gender == "Female").all()
        q2 = AtheleteDb(session).query.filter(Athelete.age > 30).all()
        q3 = (
            AtheleteDb(session)
            .query.filter(
                Athelete.gender == "Male", Athelete.age > 25, Athelete.gold_medals >= 2
            )
            .all()
        )
        print(f"Количество атлетов женщин = {len(q1)}")
        print(f"Количество атлетов старше 30 = {len(q2)}")
        print(
            f"Количество атлетов мужчин старше 25 лет получили 2 и более золотых медали = {len(q3)}"
        )


def delete_all_users(args):
    """
    удалить все записи в таблице user
    """
    with session_context() as session:
        conn = UserDb(session)
        for item in conn:
            session.delete(item)


if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser(description="Работа с базой юзерс")
    # субпарсер
    subparsers = parser.add_subparsers()
    # субпарсер add
    add_parser = subparsers.add_parser("add", help="добавление нового пользователя")
    # перечисляю аргументы
    add_parser.add_argument("first_name", type=str, help="имя пользователя")
    add_parser.add_argument("last_name", type=str, help="фамилия пользователя")
    add_parser.add_argument("gender", type=str, help="пол пользователя")
    add_parser.add_argument("email", type=str, help="email пользователя")
    add_parser.add_argument(
        "birthdate",
        type=str,
        help="дата рождения пользователя в формате ISO YYYY-MM-DD",
    )
    add_parser.add_argument("height", type=float, help="рост пользователя в метрах")
    # подключение функции add к субпарсеру
    add_parser.set_defaults(func=add)

    # субпарсер query_id
    query_id_parser = subparsers.add_parser(
        "query_id", help='запрос id по first_name и "last_name"'
    )
    query_id_parser.add_argument("first_name", type=str, help="имя пользователя")
    query_id_parser.add_argument("last_name", type=str, help="фамилия пользователя")
    query_id_parser.set_defaults(func=query_id)

    # субпарсер show
    show_users_parser = subparsers.add_parser("show", help="список пользователей")
    show_users_parser.set_defaults(func=show_users)

    # субпарсер delete_users
    delete_users_parser = subparsers.add_parser(
        "delete_users", help="удалить все записи в таблице user"
    )
    delete_users_parser.set_defaults(func=delete_all_users)

    # субпарсер query_athelete
    query_athelete_parser = subparsers.add_parser(
        "query_athelete", help="ответы на вопросы по домашнему заданию"
    )
    query_athelete_parser.set_defaults(func=query_athelete)

    args = parser.parse_args()
    if not vars(args):
        parser.print_usage()
    else:
        args.func(args)
