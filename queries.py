from sqlalchemy import create_engine, func, and_, or_
from seed import Company
from sqlalchemy.orm import sessionmaker

engine = create_engine('sqlite:///dow_jones.db', echo=True)
Session = sessionmaker(bind=engine)
session = Session()

def return_apple():
    query = session.query(Company).filter_by(company = 'Apple')
    return query.first()

def return_disneys_industry():
    query = session.query(Company).filter_by(company = 'Walt Disney')
    disney = query.first()
    return disney.industry
    #why can't we do query = session.query(Company.industry).filter_by(company = 'Walt Disney')?? what are we returning
    #with this? a query object? if so, why were we able to set california's population for the previous lab without .first()?

def return_list_of_company_objects_ordered_alphabetically_by_symbol():
    # return session.query(Company).all().order_by('symbol') WHY DOESN'T THIS WORK?
    companies = session.query(Company).order_by(Company.symbol) #why can't you just do symbol?
    return companies

def return_list_of_dicts_of_tech_company_names_and_their_EVs_ordered_by_EV_descending():
    companies = session.query(Company).filter(Company.industry == "Technology").order_by(Company.enterprise_value.desc()).all()
    company_list = [{'company': company.company, 'EV': company.enterprise_value} for company in companies]
    return company_list

def return_list_of_consumer_products_companies_with_EV_above_225():
    companies = session.query(Company).filter(and_(Company.industry == "Consumer products", Company.enterprise_value > 225)).all()
    company_list = [{'name': company.company} for company in companies]
    return company_list

def return_conglomerates_and_pharmaceutical_companies():
    companies = session.query(Company).filter(or_(Company.industry == "Conglomerate", Company.industry == "Pharmaceuticals")).all()
    company_list = [company.company for company in companies]
    return company_list

def avg_EV_of_dow_companies():
    average = session.query(func.avg(Company.enterprise_value)).first()
    return average

def return_industry_and_its_total_EV():
    industry_list = session.query(Company.industry, func.sum(Company.enterprise_value)).group_by(Company.industry).all()
    return industry_list
