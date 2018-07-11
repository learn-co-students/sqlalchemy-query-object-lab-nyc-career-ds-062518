from sqlalchemy import create_engine, func
from seed import Company
from sqlalchemy.orm import sessionmaker

engine = create_engine('sqlite:///dow_jones.db', echo=True)
Session = sessionmaker(bind=engine)
session = Session()

def return_apple():
    return session.query(Company).filter_by(company = 'Apple')[0]

def return_disneys_industry():
    disney = session.query(Company).filter_by(company = 'Walt Disney')[0]
    return disney.industry

def return_list_of_company_objects_ordered_alphabetically_by_symbol():
    return session.query(Company).order_by(Company.symbol).all()

def return_list_of_dicts_of_tech_company_names_and_their_EVs_ordered_by_EV_descending():
    ev_list = []
    ev_dict = {}
    for comp in session.query(Company).filter_by(industry = 'Technology').order_by(Company.enterprise_value.desc()).all():
        ev_dict['company'] = comp.company
        ev_dict['EV'] = comp.enterprise_value
        ev_list.append(ev_dict)
        ev_dict = {}
    return ev_list

def return_list_of_consumer_products_companies_with_EV_above_225():
    cp_list = []
    cp_dict = {}
    for comp in session.query(Company).filter_by(industry = 'Consumer products').filter(Company.enterprise_value > 225).all():
        cp_dict['name'] = comp.company
        cp_list.append(cp_dict)
        cp_dict = {}
    return cp_list

def return_conglomerates_and_pharmaceutical_companies():
    cong_list = [comp.company for comp in session.query(Company).filter(Company.industry == 'Conglomerate').all()]
    pharma_list = [comp.company for comp in session.query(Company).filter(Company.industry == 'Pharmaceuticals').all()]
    return sorted(cong_list + pharma_list)

def avg_EV_of_dow_companies():
    return session.query(func.avg(Company.enterprise_value))[0]

def return_industry_and_its_total_EV():
    return session.query(Company.industry,func.sum(Company.enterprise_value)).group_by(Company.industry).all()
