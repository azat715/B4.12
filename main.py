from contextlib import contextmanager
from dataclasses import dataclass, InitVar, astuple
from datetime import date, timedelta
import re

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, String, Date
from sqlalchemy.orm import sessionmaker


# магия регэксп
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

# ORM модель двух баз атлетов и юзеров
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
        return "<Athelete(age='%s', birthdate='%s', gender='%s', \
        height='%s', name='%s', weight='%s', gold_medals='%s', \
        silver_medals='%s', total_medals='%s',  sport='%s',  country='%s')>" % (
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


class User(Base):
    __tablename__ = "user"

    id = Column(Integer, primary_key=True)
    first_name = Column(String)
    last_name = Column(String)
    gender = Column(String)
    email = Column(String)
    birthdate = Column(Date)
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


# коннект к базе
eng = create_engine(DB_PATH)

# таблицы
print(eng.table_names())

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

    def __init__(self, orm_class):
        self.orm_class = orm_class
        with session_context() as s:
            self.s = s

    def __iter__(self):
        return iter(self.s.query(self.orm_class).all())

    def __len__(self):
        return len(self.s.query(self.orm_class).all())

    @property
    def query(self):
        return self.s.query(self.orm_class)


class UserDb(ManagerDb):
    def __init__(self):
        super().__init__(User)

    def append(self, orm_class):
        self.s.add(orm_class)
        self.s.commit()
        return None


class AtheleteDb(ManagerDb):
    def __init__(self):
        super().__init__(Athelete)


#  дальше разбор ввода пользователя


def age_calculation(birthdate: date) -> int:
    """
    вычисление полных лет пользователя
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
    """
    if re.match(regex, email):
        return True
    else:
        return False


@dataclass
class UserInput:
    """
    парсер ввода пользователя
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
            raise ValueError("Значение роста юзера больше нуля и меньше 2.50 м")


def main(user: UserInput) -> None:
    record = User(
        first_name=user.first_name,
        last_name=user.last_name,
        gender=user.gender,
        email=user.email,
        birthdate=user.birthdate,
        height=user.height,
    )
    print(UserDb().append(record))
    print("1")


if __name__ == "__main__":
    # q = ManagerDb(Athelete).query.filter(Athelete.gender == 'Female').all()
    # q2 = ManagerDb(Athelete).query.filter(Athelete.age > 30).all()
    # q3 = ManagerDb(Athelete).query.filter(Athelete.gender == 'Male', Athelete.age > 25, Athelete.gold_medals >= 2).all()
    # print(f"Количество атлетов женщин = {len(q)}")
    # print(f"Количество атлетов старше 30 = {len(q2)}")
    # print(f"Количество атлетов мужчин старше 25 лет получили 2 и более золотых медали = {len(q3)}")
    # for item in AtheleteDb():
    #     print(item)

    import argparse

    parser = argparse.ArgumentParser(description="Добавление юзера в базу данных")
    # перечисляю аргументы
    parser.add_argument("first_name", type=str, help="имя пользователя")
    parser.add_argument("last_name", type=str, help="фамилия пользователя")
    parser.add_argument("gender", type=str, help="пол пользователя")
    parser.add_argument("email", type=str, help="email пользователя")
    parser.add_argument(
        "birthdate",
        type=str,
        help="дата рождения пользователя в формате ISO YYYY-MM-DD",
    )
    parser.add_argument("height", type=float, help="рост пользователя в метрах")
    args = parser.parse_args()
    user = UserInput(
        args.first_name,
        args.last_name,
        args.gender,
        args.email,
        args.height,
        birthdate_raw=args.birthdate,
    )
    main(user)
    print("3")
    for item in UserDb():
        print(item)