from sqlalchemy import create_engine, func
from seed import Company
from sqlalchemy.orm import sessionmaker
#from sqlalchemy import desc

engine = create_engine('sqlite:///dow_jones.db', echo=True)
Session = sessionmaker(bind=engine)
session = Session()

def return_apple():
    apple = session.query(Company).filter_by(company = "Apple").first()
    return apple

def return_disneys_industry():
    disney_industry = session.query(Company).filter_by(company = "Walt Disney").first()
    return disney_industry.industry

def return_list_of_company_objects_ordered_alphabetically_by_symbol():
     return session.query(Company).order_by((Company.symbol.asc()))

def return_list_of_dicts_of_tech_company_names_and_their_EVs_ordered_by_EV_descending():
    x = [{'company' : i.company, 'EV': i.enterprise_value} for i in
    session.query(Company).filter_by(industry='Technology').order_by(Company.enterprise_value.desc())]
    return x

def return_list_of_consumer_products_companies_with_EV_above_225():
    return [{'name' : i.company} for i in session.query(Company).filter_by(industry="Consumer products")
    if i.enterprise_value > 225]

def return_conglomerates_and_pharmaceutical_companies():
    return [i.company for i in session.query(Company) if i.industry == 'Pharmaceuticals' or i.industry == 'Conglomerate']

def avg_EV_of_dow_companies():
    return session.query(func.avg(Company.enterprise_value)).first()

def return_industry_and_its_total_EV():
    return session.query(Company.industry, func.sum(Company.enterprise_value)).group_by(Company.industry).order_by(Company.industry.asc()).all()
