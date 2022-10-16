import csv
from collections import defaultdict
from datetime import datetime as dt
import os

import sqlalchemy as db
from sqlalchemy.orm import declarative_base, Session

TIME_FORMAT = '%Y-%m-%dT%H-%M-%S'

Base = declarative_base()
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class Pep(Base):
    __tablename__ = 'pep'
    id = db.Column(db.Integer, primary_key=True)
    number = db.Column(db.Integer)
    name = db.Column(db.String(200))
    status = db.Column(db.String(50))


class PepParsePipeline:
    status_count = defaultdict(int)
    total = 0

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
        if item.get('status'):
            self.total += 1
            self.status_count[
                item['status']] = self.status_count.get(
                item['status'], 0) + 1
        return item


    def close_spider(self, spider):
        now_time = dt.now().strftime(TIME_FORMAT)
        filename = os.path.join(
            BASE_DIR,
            'results',
            f'status_summary_{now_time}.csv'
        )
        results = [('Cтатус', 'Количество')]
        results.extend(self.status_count.items())
        results.append(('Total: ', self.total))
        with open(filename, mode='w', encoding='utf-8') as f:
            db.select([
                Pep.status,
                db.func.count(Pep.status).label('status_count')
            ]).group_by(Pep.status)
            writer = csv.writer(f)
            writer.writerows(results)
        self.session.close()
