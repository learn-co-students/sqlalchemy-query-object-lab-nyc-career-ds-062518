from sqlalchemy import create_engine, func, and_, or_
from seed import Company
from sqlalchemy.orm import sessionmaker

engine = create_engine('sqlite:///dow_jones.db', echo=True)
Session = sessionmaker(bind=engine)
session = Session()

# query = session.query(Company).filter_by(company = 'Apple')

def return_apple():
    query = session.query(Company).filter_by(company = 'Apple')
    return query.first()

    #return session.query(Company).first()???

def return_disneys_industry():
    query = session.query(Company).filter_by(company = "Walt Disney")
    disney = query.first()
    return disney.industry

def return_list_of_company_objects_ordered_alphabetically_by_symbol():
    company_list = session.query(Company).order_by(Company.symbol)
    return company_list
    # ordered_companies = companies.order_by(company.symbol)
    # return ordered_companies

def return_list_of_dicts_of_tech_company_names_and_their_EVs_ordered_by_EV_descending():
    company_list = session.query(Company).filter(Company.industry == 'Technology').order_by(Company.enterprise_value.desc()).all()

    final_company_list = [{'company': company.company, 'EV': company.enterprise_value} for company in company_list]

    return final_company_list
    #
    # for name, ev in sorted_company_list:
    #     company_dict = {}
    #     company_dict['company'] = name
    #     company_dict['EV'] = ev
    #     final_company_list.append(company_dict)
    # company_dict = [dict(Company) for Company in companies]
    # return company_dict

def return_list_of_consumer_products_companies_with_EV_above_225():
    company_list = session.query(Company).filter(and_(Company.industry == 'Consumer products', Company.enterprise_value > 225)).all()

    final_company_list = [{'name': company.company} for company in company_list]

    return final_company_list

def return_conglomerates_and_pharmaceutical_companies():
    company_list = session.query(Company).filter(or_(Company.industry == 'Conglomerate', Company.industry == 'Pharmaceuticals')).all()

    final_company_list = [company.company for company in company_list]

    return final_company_list

def avg_EV_of_dow_companies():
    dow_ev_avg = session.query(func.avg(Company.enterprise_value)).first()
    return dow_ev_avg

def return_industry_and_its_total_EV():
    industry_evs = session.query(Company.industry, func.sum(Company.enterprise_value)).group_by(Company.industry).all()
    return industry_evs
