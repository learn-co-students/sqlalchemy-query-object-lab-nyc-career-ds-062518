from sqlalchemy import create_engine, func, or_
from seed import Company
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import label

engine = create_engine('sqlite:///dow_jones.db', echo=True)
Session = sessionmaker(bind=engine)
session = Session()

def return_apple():
    return session.query(Company).filter_by(company = 'Apple')[0]
def return_disneys_industry():
    return session.query(Company).filter_by(company = 'Walt Disney')[0].industry
def return_list_of_company_objects_ordered_alphabetically_by_symbol():
    return session.query(Company).order_by(Company.symbol).all()
def return_list_of_dicts_of_tech_company_names_and_their_EVs_ordered_by_EV_descending():
    ev_company = session.query(Company).filter_by(industry = 'Technology').order_by(Company.enterprise_value.desc()).all()
    return [{'company':element.company, 'EV':element.enterprise_value} for element in ev_company]
def return_list_of_consumer_products_companies_with_EV_above_225():
    ev_company = session.query(Company).filter(Company.enterprise_value > 225, Company.industry == 'Consumer products').all()
    return [{'name':element.company} for element in ev_company]
def return_conglomerates_and_pharmaceutical_companies():
    ev_company = session.query(Company).filter((Company.industry == 'Conglomerate') | (Company.industry == 'Pharmaceuticals'))
    return [element.company for element in ev_company]
def avg_EV_of_dow_companies():
    return session.query(func.avg(Company.enterprise_value))[0]
def return_industry_and_its_total_EV():
    return session.query(Company.industry,func.sum(Company.enterprise_value)).group_by(Company.industry).order_by(Company.industry).all()
