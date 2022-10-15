from datetime import datetime as dt
import os

import sqlalchemy as db
from sqlalchemy.orm import declarative_base, Session

Base = declarative_base()
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class Pep(Base):
    __tablename__ = 'pep'
    id = db.Column(db.Integer, primary_key=True)
    number = db.Column(db.Integer)
    name = db.Column(db.String(200))
    status = db.Column(db.String(50))


class PepParsePipeline:
    def open_spider(self, spider):
        engine = db.create_engine('sqlite:///posts.db')
        Base.metadata.create_all(engine)
        self.session = Session(engine)

    def process_item(self, item, spider):
        pep = Pep(
            number=item['number'],
            name=item['name'],
            status=item['status'],
        )
        if not self.session.query(Pep).filter(Pep.number == pep.number).all():
            self.session.add(pep)
            self.session.commit()
        return item

    def close_spider(self, spider):
        filename = os.path.join(
            BASE_DIR,
            'results',
            f'status_summary_{dt.now().strftime("%Y-%m-%dT%H-%M-%S")}.csv'
        )
        with open(filename, mode='w', encoding='utf-8') as f:
            f.write('Статус,Количество\n')
            total = 0
            for status, status_count in self.session.execute(
                db.select([
                    Pep.status,
                    db.func.count(Pep.status).label('status_count')
                ]).group_by(Pep.status)
            ):
                f.write(f'{status},{status_count}\n')
                total += status_count
            f.write(f'Total,{total}\n')
        self.session.close()