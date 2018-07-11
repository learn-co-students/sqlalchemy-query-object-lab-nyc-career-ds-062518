from sqlalchemy import create_engine, func
from seed import Company
from sqlalchemy.orm import sessionmaker

engine = create_engine('sqlite:///dow_jones.db', echo=True)
Session = sessionmaker(bind=engine)
session = Session()

def return_apple():
    return session.query(Company).filter_by(company = 'Apple').first()

def return_disneys_industry():
    dis = session.query(Company).filter_by(company = 'Walt Disney').first()
    return dis.industry

def return_list_of_company_objects_ordered_alphabetically_by_symbol():
    return session.query(Company).order_by(Company.symbol).all()

def return_list_of_dicts_of_tech_company_names_and_their_EVs_ordered_by_EV_descending():
    comp = session.query(Company).filter_by(industry='Technology').order_by(Company.enterprise_value.desc()).all()
    comp_list = []
    for stock in comp:
        comp_list.append({"company" : stock.company, "EV":stock.enterprise_value})
    return comp_list

def return_list_of_consumer_products_companies_with_EV_above_225():
    comp = session.query(Company).filter_by(industry='Consumer products').filter(Company.enterprise_value > 225).all()
    comp_list = []
    for stock in comp:
        comp_list.append({"name" : stock.company})
    return comp_list

def return_conglomerates_and_pharmaceutical_companies():
#     return session.query(Company).filter(Company.industry=='Conglomerate' or Company.industry =='Pharmaceuticals').all()
    comp = session.query(Company).all()
    return [stock.company for stock in comp if stock.industry=='Conglomerate' or stock.industry =='Pharmaceuticals']


def avg_EV_of_dow_companies():
    return session.query(func.avg(Company.enterprise_value)).first()


def return_industry_and_its_total_EV():
#alpha order indusrty list and gets its EV
    return session.query(Company.industry, func.sum(Company.enterprise_value)).group_by(Company.industry).all()
