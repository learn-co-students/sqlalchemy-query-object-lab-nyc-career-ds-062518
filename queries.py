from sqlalchemy import create_engine, func
from seed import Company
from sqlalchemy.orm import sessionmaker

engine = create_engine('sqlite:///dow_jones.db', echo=True)
Session = sessionmaker(bind=engine)
session = Session()

def return_apple():
    apple = session.query(Company).filter_by(company = "Apple").first()
    return apple
    pass

def return_disneys_industry():
    disney = session.query(Company).filter_by(company = "Walt Disney").first()
    return disney.industry
    pass

def return_list_of_company_objects_ordered_alphabetically_by_symbol():
    return session.query(Company).order_by(Company.symbol.asc())
    pass

def return_list_of_dicts_of_tech_company_names_and_their_EVs_ordered_by_EV_descending():
    tech = session.query(Company.company, Company.enterprise_value).filter_by(industry = "Technology")
    return [{"company": i.company, "EV":i.enterprise_value} for i in tech.order_by(Company.enterprise_value.desc())]
    pass

def return_list_of_consumer_products_companies_with_EV_above_225():
    consumer_products = session.query(Company.company, Company.enterprise_value).filter_by(industry = "Consumer products")
    return [{"name":i.company,} for i in consumer_products if i.enterprise_value>225]
    pass

def return_conglomerates_and_pharmaceutical_companies():
    return [i.company for i in session.query(Company) if i.industry == "Conglomerate" or i.industry == "Pharmaceuticals"]
    pass

def avg_EV_of_dow_companies():
    return session.query(func.avg(Company.enterprise_value)).first()
    pass

def return_industry_and_its_total_EV():
    industry_EV = session.query(Company.industry, func.sum(Company.enterprise_value)).group_by(Company.industry).order_by(Company.industry.asc()).all()
    return industry_EV
    pass
