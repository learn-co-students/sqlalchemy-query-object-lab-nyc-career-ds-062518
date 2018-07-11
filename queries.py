from sqlalchemy import create_engine, func
from seed import Company
from sqlalchemy.orm import sessionmaker

engine = create_engine('sqlite:///dow_jones.db', echo=True)
Session = sessionmaker(bind=engine)
session = Session()

def return_apple():
    apple = session.query(Company).filter_by(company = 'Apple')[0]
    return apple

def return_disneys_industry():
    disney = session.query(Company).filter_by(company = 'Walt Disney')[0]
    industry = disney.industry
    return industry

def return_list_of_company_objects_ordered_alphabetically_by_symbol():
    company_objects = session.query(Company).order_by(Company.symbol.asc()).all()
    return company_objects

def return_list_of_dicts_of_tech_company_names_and_their_EVs_ordered_by_EV_descending():
    ev_dict = {}
    ev_list = []
    company_objects = session.query(Company).filter_by(industry = 'Technology').order_by(Company.enterprise_value.desc()).all()
    for item in company_objects:
        ev_dict['company'] = item.company
        ev_dict['EV'] = item.enterprise_value
        ev_list.append(ev_dict)
        ev_dict = {}
    return ev_list

def return_list_of_consumer_products_companies_with_EV_above_225():
    ev_dict = {}
    ev_list = []
    company_objects = session.query(Company).filter_by(industry = 'Consumer products').filter(Company.enterprise_value > 225).all()
    for comp in company_objects:
        ev_dict['name'] = comp.company
        ev_list.append(ev_dict)
        ev_dict = {}
    return ev_list

def return_conglomerates_and_pharmaceutical_companies():
    cong_list = [comp.company for comp in session.query(Company).filter(Company.industry == 'Conglomerate').all()]
    pharm_list = [comp.company for comp in session.query(Company).filter(Company.industry == 'Pharmaceuticals').all()]
    company_objects = cong_list + pharm_list
    return sorted(company_objects)

def avg_EV_of_dow_companies():
    return session.query(func.avg(Company.enterprise_value))[0]

def return_industry_and_its_total_EV():
    ev_list = session.query(Company.industry, func.sum(Company.enterprise_value)).group_by(Company.industry).all()
    return ev_list
