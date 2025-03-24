import traceback
import pyodbc
import psycopg2
import logging
from psycopg2.extras import RealDictCursor
from datetime import datetime
from enum import Enum

class Database:
    def __init__(self, host, database, user, password, port=None):
        self.host= host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
    
    def display_info(self):
        return f"Database: Host={self.host}, database={self.database}, user={self.user}, password={self.password}, port={self.port}"
    
    def getConnectionString(self, type):
        if type == DatabaseType.MSSQL:
            return f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.host};DATABASE={self.database};UID={self.user};PWD={self.password}'
        elif type == DatabaseType.PostgreSQL:
            return f"host='{self.host}' dbname='{self.database}' user='{self.user}' password='{self.password}' port='{self.port}'"
  
  
#region Configs

isDebug = False
start = '2024-01-01'
end = '2024-01-01'
logFile = "C:/Ivan/python/importcrm/importcrm.log"

orderBusDB = Database('matrix-prod.csz618uhxdfr.ap-east-1.rds.amazonaws.com', 'Orders', 'admin', 'Passw0rd')
orderBBDB = Database('matrix-prod.csz618uhxdfr.ap-east-1.rds.amazonaws.com', 'Orders_BB', 'admin', 'Passw0rd')
#odooDB = Database('16.163.108.1', 'odoo', 'odoo', 'Aamlaaml17!@#', '5632')
odooDB = Database('localhost', 'odoohk', 'odoo', 'odoo', '5632')
crmDB = Database('47.243.253.190', 'CRM', 'postgres', 'mysecretpassword', '5432')

#endregion

#region Function Logging

def log_function_call(func):
    def wrapper(*args, **kwargs):
        logging.info(f"Start function: {func.__name__} | Args: {args}")
        result = func(*args, **kwargs)
        logging.info(f"End function {func.__name__}")
        return result
    return wrapper

#endregion

#region Objects/Enum
   
class OdooCustomerData:
    def __init__(self, type, company_id, company_name, contact_id, contact_name):
        self.type = type
        self.company_id = company_id
        self.company_name = company_name
        self.contact_id = contact_id
        self.contact_name = contact_name
        
    def display_info(self):
        return f"OdooCustomer: Type={self.type}, company_id={self.company_id}, company_name={self.company_name}, contact_id={self.contact_id}, contact_name={self.contact_name}"
        
class CRMCustomerData:
    def __init__(self, type, company_id, company_name, client_type=None, agency_type=None, company_nature=None, street=None, street2=None, street3=None, status=None, business_category=None, contact_id=None, contact_title=None, contact_first_name=None, contact_last_name=None, contact_function=None, contact_phone=None, contact_mobile=None, contact_email=None, contact_status=None):
        self.type = type
        self.company_id = company_id
        self.company_name = company_name
        self.client_type = client_type
        self.agency_type = agency_type
        self.company_nature = company_nature
        self.street = street
        self.street2 = street2
        self.street3 = street3
        self.status = status
        self.business_category = business_category
        self.contact_id = contact_id
        self.contact_title = contact_title
        self.contact_first_name = contact_first_name
        self.contact_last_name = contact_last_name
        self.contact_function = contact_function
        self.contact_phone = contact_phone
        self.contact_mobile = contact_mobile
        self.contact_email = contact_email
        #self.contact_display_name = contact_display_name
        self.contact_status = contact_status
        
    def display_info(self):
        return f"CrmCustomer: type={self.type}, company_id={self.company_id}, company_name={self.company_name}, client_type={self.client_type}, agency_type={self.agency_type}, company_nature={self.company_nature}, street={self.street}, street2={self.street2}, street3={self.street3}, status={self.status}, business_category={self.business_category}, contact_id={self.contact_id}, contact_title={self.contact_title}, contact_first_name={self.contact_first_name}, contact_last_name={self.contact_last_name}, contact_function={self.contact_function}, , contact_phone={self.contact_phone}, contact_mobile={self.contact_mobile}, contact_email={self.contact_email}, contact_status={self.contact_status}"

class CustomerDataType(Enum):
    Company = 1
    Contact = 2
    All = 3

class CustomerDataStatus(Enum):
    Active = 1
    Inactive = 2
    All = 3
    
class DatabaseType(Enum):
    MSSQL = 1
    PostgreSQL = 2

#endregion

#region Functions

@log_function_call
def getCustomerDataFromOdoo(connOdoo, customerDataType, customerDataStatus, editStartDate='2020-01-01', editEndDate='2099-12-31', company_name=None):
    curOdooCustomerData = connOdoo.cursor(cursor_factory=RealDictCursor)
    queryOdooCustomerData = (
        f"SELECT "
        f"CASE WHEN a.is_company = 't' THEN 'Company' ELSE 'Contact' END AS \"type\", "
        f"CASE WHEN a.is_company = 't' THEN a.id ELSE b.id END AS company_id, "
        f"CASE WHEN a.is_company = 't' THEN TRIM(REPLACE(REPLACE(a.name, '’', ''''), '''', '''''')) ELSE TRIM(REPLACE(REPLACE(b.name, '’', ''''), '''', '''''')) END AS company_name, "
        f"CASE WHEN a.is_company = 't' THEN NULL ELSE a.id END AS contact_id, "
        f"CASE WHEN a.is_company = 't' THEN NULL ELSE TRIM(a.name) END AS contact_name "
        f"FROM res_partner AS a "
        f"LEFT JOIN res_partner AS b ON a.parent_id = b.id "
        f"LEFT JOIN res_users AS u ON a.id = u.partner_id "
        f"WHERE "
        f"{ 'a.is_company=\'t\' AND ' if customerDataType == CustomerDataType.Company else 'a.is_company=\'f\' AND ' if customerDataType == CustomerDataType.Contact else '' } "
        f"{ f'LOWER(a.name)=\'{company_name.strip().casefold()}\' AND ' if company_name is not None else '' } "
        f"{ 'a.active=\'t\' AND ' if customerDataStatus == CustomerDataStatus.Active else 'a.active=\'f\' AND ' if customerDataStatus == CustomerDataStatus.Inactive else '' } "       
        f"((a.write_date >= '{editStartDate} 00:00:00' AND a.write_date < '{editEndDate} 23:59:59') or a.write_date is NULL) "
        f"AND u.Login IS NULL "
        f"ORDER BY a.id desc, b.id desc;"
    )
    logging.info(f"Get Odoo Customer Company Data Query: {queryOdooCustomerData}")
    curOdooCustomerData.execute(queryOdooCustomerData)
    resultsOdooCustomerData = curOdooCustomerData.fetchall()
    curOdooCustomerData.close()
    
    odooCustomerDataList = []
    for rowOdooCustomerData in resultsOdooCustomerData:
        odooCustomerData = OdooCustomerData(
            type=rowOdooCustomerData['type'],
            company_id=rowOdooCustomerData['company_id'],
            company_name=rowOdooCustomerData['company_name'],
            contact_id=rowOdooCustomerData['contact_id'],
            contact_name=rowOdooCustomerData['contact_name']
        )
        odooCustomerDataList.append(odooCustomerData)
    return odooCustomerDataList

@log_function_call
def getCustomerDataFromCRM(connCrm, customerDataType, customerDataStatus, editStartDate='2020-01-01', editEndDate='2099-12-31', company_id=None):
    if  customerDataType == customerDataType.Company:
        curCrmCompanyData = connCrm.cursor(cursor_factory=RealDictCursor)
        queryCrmCompanyData = (
            f"SELECT DISTINCT "
            f"c.id AS company_id, "
            f"TRIM(REPLACE(REPLACE(c.\"name\"->>'en_US', '’', ''''), '''', '''''')) AS company_name, "
            f"ct.name->>'en_US' AS client_type, "
            f"at.name->>'en_US' AS agency_type, "
            f"(CASE WHEN c.\"agencyId\" IS NOT NULL THEN 'Other' ELSE n.\"name\"->>'en_US' END) AS company_nature, "
            f"REPLACE(REPLACE(ca.address1, '’', ''''), '''', '''''') AS street, "
            f"REPLACE(REPLACE(ca.address2, '’', ''''), '''', '''''') AS street2, "
            f"REPLACE(REPLACE(ca.address3, '’', ''''), '''', '''''') AS street3, "
            f"c.status AS status, "
            f"STRING_AGG(cbc.\"buesinessCategoryLevelTwoId\"::text, ',') AS business_category "
            f"FROM \"CRM\".\"companies\" AS c "
            f"LEFT JOIN \"CRM\".\"companyTypes\" AS ct ON c.\"companyType\" = ct.\"id\" "
            f"LEFT JOIN \"CRM\".\"natures\" AS n ON c.\"natureId\" = n.\"id\" "
            f"LEFT JOIN ( "
            f"SELECT "
            f"ca.\"parentId\", "
            f"ca.address1, "
            f"ca.address2, "
            f"ca.address3, "
            f"ROW_NUMBER() OVER (PARTITION BY ca.\"parentId\" ORDER BY ca.\"createdAt\" DESC) AS rn "
            f"FROM \"CRM\".\"companyAddresses\" AS ca "
            f") AS ca ON c.\"id\" = ca.\"parentId\" AND ca.rn = 1 "
            f"LEFT JOIN \"CRM\".\"companyBusinessCategories\" AS cbc ON c.\"id\" = cbc.\"companyId\" "
            f"LEFT JOIN \"CRM\".\"agencyTypes\" AS at ON c.\"agencyId\" = at.\"id\" "
            f"WHERE "
            
            f"{ "LOWER(REPLACE(REPLACE(c.\"name\"->>'en_US', '’', ''''), '''', '''''')) like N'%denuo limited%' and"  if isDebug else '' } "
            
            f"{ 'c.status=\'t\' AND ' if customerDataStatus == CustomerDataStatus.Active else 'c.status=\'f\' AND ' if customerDataStatus == CustomerDataStatus.Inactive else '' } "
            f"{ f'c.id=\'{company_id}\' AND ' if company_id is not None else '' } "
            f"c.\"editedAt\" >= '{editStartDate} 00:00:00' AND c.\"editedAt\" < '{editEndDate} 23:59:59' "
            f"GROUP BY c.\"name\"->>'en_US', ct.\"name\"->>'en_US', at.\"name\"->>'en_US', c.\"agencyId\", "
            f"(CASE WHEN c.\"agencyId\" IS NOT NULL THEN 'Other' ELSE n.\"name\"->>'en_US' END), "
            f"ca.address1, ca.address2, ca.address3, c.status, c.id "
            f"ORDER BY c.id desc;"
        )
        logging.info(f"Get CRM Customer Data Query : {queryCrmCompanyData}")
        curCrmCompanyData.execute(queryCrmCompanyData)
        resultsCRMCompanyData = curCrmCompanyData.fetchall()
        curCrmCompanyData.close()
        
        crmCustomerDataList = []
        for rowCRMCompanyData in resultsCRMCompanyData:
            crmCustomerData = CRMCustomerData(
                type='Company',
                company_id=rowCRMCompanyData['company_id'],
                company_name=rowCRMCompanyData['company_name'],
                client_type=rowCRMCompanyData['client_type'],
                agency_type=rowCRMCompanyData['agency_type'],
                company_nature=rowCRMCompanyData['company_nature'],
                street=rowCRMCompanyData['street'],
                street2=rowCRMCompanyData['street2'],
                street3=rowCRMCompanyData['street3'],
                status=rowCRMCompanyData['status'],
                business_category=rowCRMCompanyData['business_category']
            )
            crmCustomerDataList.append(crmCustomerData)
    else: # Contact
        curCrmContactData = connCrm.cursor(cursor_factory=RealDictCursor)
        queryCrmContactData = (
            f"SELECT DISTINCT "
            f"c.id AS company_id, "
            f"TRIM(REPLACE(REPLACE(c.\"name\"->>'en_US', '’', ''''), '''', '''''')) AS company_name, "
            f"ct.id AS contact_id, "
            f"t.\"name\"->>'en_US' AS contact_title, "
            f"TRIM(ct.\"name\"->'en_US'->>'first') AS contact_first_name, "
            f"TRIM(ct.\"name\"->'en_US'->>'last') AS contact_last_name, "
            f"ct.\"jobTitle\" AS contact_function, "
            f"ct.\"directLine\" AS contact_phone, "
            f"ct.\"mobileNo\" AS contact_mobile, "
            f"ct.\"email\" AS contact_email, "
            f"ct.\"status\" AS contact_status "
            f"FROM \"CRM\".\"contacts\" AS ct "
            f"LEFT JOIN \"CRM\".\"companies\" AS c ON ct.\"companyId\" = c.\"id\" "
            f"LEFT JOIN \"CRM\".\"titles\" AS t ON ct.\"titleId\" = t.\"id\" "
            f"WHERE "
            
            f"{ "ct.\"name\"->'en_US'->>'first' LIKE \'%Jeffrey%\' and "  if isDebug else '' } "
            f"{ "ct.\"name\"->'en_US'->>'last' LIKE \'%Lee%\' and "  if isDebug else '' } "
            
            f"{ 'c.status=\'t\' AND ' if customerDataStatus == CustomerDataStatus.Active else 'c.status=\'f\' AND ' if customerDataStatus == CustomerDataStatus.Inactive else '' }"
            f"ct.\"editedAt\" >= '{start} 00:00:00' AND ct.\"editedAt\" < '{end} 23:59:59';"
        )
        logging.info(f"Get CRM Customer Contact Data Query: {queryCrmContactData}")
        curCrmContactData.execute(queryCrmContactData)
        resultsCRMContactData = curCrmContactData.fetchall()
        curCrmContactData.close()
        
        crmCustomerDataList = []
        for rowCRMContactData in resultsCRMContactData:
            crmCustomerData = CRMCustomerData(
                type='Contact',
                company_id=rowCRMContactData['company_id'],
                company_name=rowCRMContactData['company_name'],
                contact_id=rowCRMContactData['contact_id'],
                contact_title=rowCRMContactData['contact_title'],
                contact_first_name=rowCRMContactData['contact_first_name'],
                contact_last_name=rowCRMContactData['contact_last_name'],
                contact_function=rowCRMContactData['contact_function'],
                contact_phone=rowCRMContactData['contact_phone'],
                contact_mobile=rowCRMContactData['contact_mobile'],
                contact_email=rowCRMContactData['contact_email'],
                contact_status=rowCRMContactData['contact_status'],
                #display_name=rowCRMContactData['display_name']
            )
            crmCustomerDataList.append(crmCustomerData)

    return crmCustomerDataList

@log_function_call
def exportCRMCustomerDataToOdoo(start='2020-01-01', end='2099-12-31'):
    connOrderBus = None
    connOrderBB = None
    connCrm = None
    connOdoo = None
    curCrm = None
    curOdoo = None
    
    try:
        currentDateTime = datetime.now()
        
        #region Connect to Databases
        logging.info("--- 1) Start Connect to Databases---")
        
        #Connect to Order db
        connOrderBus = pyodbc.connect(orderBusDB.getConnectionString(DatabaseType.MSSQL))
        logging.info(orderBusDB.display_info())
        
        connOrderBB = pyodbc.connect(orderBBDB.getConnectionString(DatabaseType.MSSQL))
        logging.info(orderBBDB.display_info())
        
        # Connect to Odoo db
        connOdoo = psycopg2.connect(odooDB.getConnectionString(DatabaseType.PostgreSQL))
        logging.info(odooDB.display_info())

        # Connect to CRM db
        connCrm = psycopg2.connect(crmDB.getConnectionString(DatabaseType.PostgreSQL))
        logging.info(crmDB.display_info())
        
        logging.info("--- 1) End Connect to Databases---")
        
        #endregion
        
        #region Import Companies
        logging.info("--- 2) Start Import Companies ---")
        
        # Get All Active Companies from Odoo
        odooActiveCompanies = getCustomerDataFromOdoo(connOdoo, CustomerDataType.Company, CustomerDataStatus.Active)
        logging.info(f"Total number of Odoo Active Companies: {len(odooActiveCompanies)}")
        for index, odooActiveCompany in enumerate(odooActiveCompanies):
            logging.info(f"Odoo Active Company {index+1}: {odooActiveCompany.display_info()}")
            
        # Get All Companies from CRM
        crmCompanies = getCustomerDataFromCRM(connCrm, CustomerDataType.Company, CustomerDataStatus.All, start, end)
        logging.info(f"Total number of CRM Companies: {len(crmCompanies)}")
        for index, crmCompany in enumerate(crmCompanies):
            logging.info(f"CRM Company {index+1}: {crmCompany.display_info()}")
                  
        #Update All Companies to Odoo
        curInsertOrUpdateOdooCompany = connOdoo.cursor()
        for index, crmActiveCompany in enumerate(crmCompanies):
            
            currentCompanyId = None
            
            #Insert or Update Company
            odooActiveCompanyNames = [odooActiveCompany.company_name.strip().casefold() for odooActiveCompany in odooActiveCompanies]
                        
            if crmActiveCompany.company_name.strip().casefold() in odooActiveCompanyNames: # Update
                
                # Get Existing Company Id
                odooActiveCompany = next(iter(sorted([odooActiveCompany for odooActiveCompany in odooActiveCompanies if odooActiveCompany.company_name.strip().casefold() == crmActiveCompany.company_name.strip().casefold()], key=lambda x: x.company_id, reverse=True)), None)
                currentCompanyId = odooActiveCompany.company_id
                
                queryUpdateCrmCompany = (
                    f"UPDATE res_partner SET "
                    f"write_uid = 1, "
                    f"ref = {f"\'Update Company at {currentDateTime.strftime("%Y-%m-%d %H:%M:%S")}: Range from {start} to {end}\'"}, "
                    f"street = {f'\'{crmActiveCompany.street}\'' if crmActiveCompany.street is not None else 'NULL'}, "
                    f"street2 = {f'\'{crmActiveCompany.street2}\'' if crmActiveCompany.street2 is not None else 'NULL'}, "
                    f"active = {crmActiveCompany.status}, "
                    f"write_date = '{currentDateTime}', "
                    f"street3 = {f'\'{crmActiveCompany.street3}\'' if crmActiveCompany.street3 is not None else 'NULL'}, "
                    f"agency_type = {f'\'{crmActiveCompany.agency_type}\'' if crmActiveCompany.agency_type is not None else 'NULL'}, "
                    f"company_nature = {f'\'{crmActiveCompany.company_nature}\'' if crmActiveCompany.company_nature is not None else 'NULL'} "
                    f"WHERE id = '{currentCompanyId}';"
                )
                logging.info(f"CRM Active Company {index+1} Update Company Query: {queryUpdateCrmCompany}")
                curInsertOrUpdateOdooCompany.execute(queryUpdateCrmCompany.strip())

            else: # Insert
                queryinsertOdooCompany = (
                    f"INSERT INTO res_partner (create_date, name, color, create_uid, write_uid, complete_name, ref, lang, tz, type, street, "
                    f"street2, commercial_company_name, active, is_company, partner_share, write_date, message_bounce, supplier_rank, "
                    f"customer_rank, invoice_warn, calendar_last_notif_ack, purchase_warn, sale_warn, street3, agency_type, company_nature) "
                    f"VALUES ('{currentDateTime}', "
                    f"{f'\'{crmActiveCompany.company_name}\'' if crmActiveCompany.company_name is not None else 'NULL'}, "
                    f"0, 1, 1, "
                    f"{f'\'{crmActiveCompany.company_name}\'' if crmActiveCompany.company_name is not None else 'NULL'}, "
                    f"{f"\'Update Company at {currentDateTime.strftime("%Y-%m-%d %H:%M:%S")}: Range from {start} to {end}\'"}, "
                    f"'en_US', 'Asia/Hong_Kong', 'contact', "
                    f"{f'\'{crmActiveCompany.street}\'' if crmActiveCompany.street is not None else 'NULL'}, "
                    f"{f'\'{crmActiveCompany.street2}\'' if crmActiveCompany.street2 is not None else 'NULL'}, "
                    f"{f'\'{crmActiveCompany.company_name}\'' if crmActiveCompany.company_name is not None else 'NULL'}, "
                    f"{crmActiveCompany.status}, 't', 't', "
                    f"'{currentDateTime}', 0, 0, 1, 'no-message', "
                    f"'{currentDateTime.strftime('%Y-%m-%d %H:%M:%S')}', 'no-message', 'no-message', "
                    f"{f'\'{crmActiveCompany.street3}\'' if crmActiveCompany.street3 is not None else 'NULL'}, "
                    f"{f'\'{crmActiveCompany.agency_type}\'' if crmActiveCompany.agency_type is not None else 'NULL'}, "
                    f"{f'\'{crmActiveCompany.company_nature}\'' if crmActiveCompany.company_nature is not None else 'NULL'}) "
                    f"RETURNING id;"
                )
                logging.info(f"CRM Active Company {index+1} Insert Company Query: {queryinsertOdooCompany}")
                curInsertOrUpdateOdooCompany.execute(queryinsertOdooCompany.strip())
                
                # Get New Company Id
                currentCompanyId = curInsertOrUpdateOdooCompany.fetchone()[0]
            
            # Update client types relation
            # queryDeleteCompanyClientType = f"""DELETE FROM client_types_res_partner_rel WHERE res_partner_id = {currentCompanyId};"""
            # logging.info(f"CRM Active Company {index+1} Delete Client Types: {queryDeleteCompanyClientType}")
            # curInsertOrUpdateOdooCompany.execute(queryDeleteCompanyClientType.strip())
            
            queryInsertCompanyClientType = f"INSERT INTO client_types_res_partner_rel (res_partner_id, client_types_id) SELECT {currentCompanyId}, (SELECT id FROM client_types WHERE code = '{crmActiveCompany.client_type.lower()}') WHERE NOT EXISTS (SELECT 1 FROM client_types_res_partner_rel WHERE res_partner_id = {currentCompanyId} AND client_types_id = (SELECT id FROM client_types WHERE code = '{crmActiveCompany.client_type.lower()}'));"
            logging.info(f"CRM Active Company {index+1} Insert Client Types: {queryInsertCompanyClientType}")
            curInsertOrUpdateOdooCompany.execute(queryInsertCompanyClientType.strip())
                
            # Update Business Category
            if crmActiveCompany.business_category:
                bcs = [bc.strip() for bc in crmActiveCompany.business_category.split(',')]
                for bc in bcs:
                    # queryDeleteBusinessCategory = f"""DELETE FROM business_category_res_partner_rel WHERE res_partner_id = {currentCompanyId};"""
                    # logging.info(f"CRM Active Company {index+1} Delete Business Category: {queryDeleteBusinessCategory}")
                    # curInsertOrUpdateOdooCompany.execute(queryDeleteBusinessCategory.strip())
            
                    queryInsertBusinessCategory = f"INSERT INTO business_category_res_partner_rel (res_partner_id, business_category_id) SELECT {currentCompanyId}, (SELECT id FROM business_category WHERE uuid = '{bc}') WHERE NOT EXISTS (SELECT 1 FROM business_category_res_partner_rel WHERE res_partner_id = {currentCompanyId} AND business_category_id = (SELECT id FROM business_category WHERE uuid = '{bc}'));"
                    logging.info(f"CRM Active Company {index+1} Insert Business Category: {queryInsertBusinessCategory}")
                    curInsertOrUpdateOdooCompany.execute(queryInsertBusinessCategory.strip())
            
        connOdoo.commit() # Commit Companies Import First
        
        logging.info("--- 2) End Import Companies ---")
        
        #endregion
        
        #region Import Contacts       
        logging.info("--- 3) Start Import Contacts ---")
        
        # Get Again All Active Contacts from Odoo
        odooActiveContacts = getCustomerDataFromOdoo(connOdoo, CustomerDataType.Contact, CustomerDataStatus.Active)
        logging.info(f"Total number of Odoo Active Contacts: {len(odooActiveContacts)}")
        for index, odooActiveContact in enumerate(odooActiveContacts):
            logging.info(f"Odoo Active Contact {index+1}: {odooActiveContact.display_info()}")
        
        # Get All Contacts from CRM
        crmContacts = getCustomerDataFromCRM(connCrm, CustomerDataType.Contact, CustomerDataStatus.All, start, end)
        logging.info(f"Total number of CRM Active Contacts: {len(crmContacts)}")
        for index, crmContact in enumerate(crmContacts):
            logging.info(f"CRM Contact {index+1}: {crmContact.display_info()}")
            
        #Update All Contacts to Odoo     
        curInsertOrUpdateOdooContact = connOdoo.cursor()
        for index, crmContact in enumerate(crmContacts):
            
            currentContactId = None
            
            #Insert or Update Contact
            crmContactName = f"{crmContact.contact_title + ' ' if crmContact.contact_title is not None else ''}{crmContact.contact_first_name} {crmContact.contact_last_name}"
            #odooActiveContactCompanyNames = [ (odooActiveContact.contact_name, odooActiveContact.company_name) for odooActiveContact in odooActiveContacts]
            
            ###
            # for oac in odooActiveContacts:
            #     if 'Jeffrey Lee' in oac.contact_name:
            #         print(oac.contact_name.strip().casefold() + '==' + crmContactName.strip().casefold() + ':' + oac.contact_name.strip().casefold() == crmContactName.strip().casefold())
            #         print(oac.company_name.strip().casefold() + '==' + crmContact.company_name.strip().casefold() + ':' + oac.company_name.strip().casefold() == crmContact.company_name.strip().casefold()      )         
            ###
            
            currentOdooActiveContact = next(iter(sorted([odooActiveContact for odooActiveContact in odooActiveContacts if odooActiveContact is not None and odooActiveContact.contact_name and odooActiveContact.company_name and odooActiveContact.contact_name.strip().casefold() == crmContactName.strip().casefold() and odooActiveContact.company_name.strip().casefold() == crmContact.company_name.strip().casefold()], key=lambda x: x.company_id, reverse=True)), None)
            
            #region Get Addresses, Business Category From Latest Order
            curOrderBusCustomerData = connOrderBus.cursor()
            queryContactOrderBus = f"""select 'Bus', o.id, p.id, o.CreateDate,
                                        (CASE WHEN o.ClientType='AG' THEN o.AgencyCode ELSE o.AdvertiserCode END) AS company_id,
                                        p.Code AS contact_id, 
                                        o.BillAddress AS address,
                                        bc.businessCategoryId from [Order] AS o join OrderContactPerson AS p on o.Id = p.OrderId
                                        join OrderBusinessCategory AS bc on o.Id = bc.OrderId
                                        where (CASE WHEN o.ClientType='AG' THEN o.AgencyCode ELSE o.AdvertiserCode END) = '{crmContact.company_id}' and
                                        p.Code='{crmContact.contact_id}' and
                                        o.status != 'Void'
                                        order by o.id desc, p.id desc"""
            curOrderBusCustomerData.execute(queryContactOrderBus.strip())
            orderBusCustomerDatas = curOrderBusCustomerData.fetchall()
                   
            curOrderBBCustomerData = connOrderBB.cursor()
            queryContactOrderBB = f"""select 'BB', o.id, p.id, o.CreateDate,
                                        (CASE WHEN o.ClientType='AG' THEN o.AgencyCode ELSE o.AdvertiserCode END) AS company_id,
                                        p.Code AS contact_id, 
                                        o.BillAddress AS address,
                                        bc.businessCategoryId from [Order] AS o join OrderContactPerson AS p on o.Id = p.OrderId
                                        join OrderBusinessCategory AS bc on o.Id = bc.OrderId
                                        where (CASE WHEN o.ClientType='AG' THEN o.AgencyCode ELSE o.AdvertiserCode END) = '{crmContact.company_id}' and
                                        p.Code='{crmContact.contact_id}' and
                                        o.status != 'Void'
                                        order by o.id desc, p.id desc"""
            curOrderBBCustomerData.execute(queryContactOrderBB.strip())
            orderBBCustomerDatas = curOrderBBCustomerData.fetchall()
                                
            # Get latest order for same company, contact but different BUs
            latestCreateDate = None
            latestOrder = None
            for record in orderBusCustomerDatas + orderBBCustomerDatas:
                if latestCreateDate is None: # First Record
                    latestOrder = record    
                elif record[3] > latestCreateDate: # Compare CreateDate
                    latestOrder = record
                
            # if no address, address leave empty
            latestStreet = None
            latestStreet2 = None
            latestStreet3 = None
            if latestOrder is not None:
                lastestOrderAddress = latestOrder.address.split("\r\n")
                latestStreet = lastestOrderAddress[0].replace("'", "''")
                if len(lastestOrderAddress) > 1:
                    latestStreet2 = lastestOrderAddress[1].replace("'", "''")
                if len(lastestOrderAddress) > 2:
                    latestStreet3 = lastestOrderAddress[2].replace("'", "''")
            
            businessCategoryId = None
            if latestOrder is not None:
                businessCategoryId = latestOrder.businessCategoryId
            
            #endregion
                    
            if currentOdooActiveContact is not None: # Update
                
                # Get Existing Contact Id
                currentContactId = currentOdooActiveContact.contact_id
                
                queryUpdateCrmContact = (
                    f"UPDATE res_partner SET "
                    f"write_uid = 1, "
                    f"ref = {f"\'Update Contact at {currentDateTime.strftime("%Y-%m-%d %H:%M:%S")}: Range from {start} to {end}\'"}, "
                    f"function= {f'\'{crmContact.contact_function}\'' if crmContact.contact_function is not None else 'NULL'}, "
                    f"street = {f'\'{latestStreet}\'' if latestStreet is not None else 'NULL'}, "
                    f"street2 = {f'\'{latestStreet2}\'' if latestStreet2 is not None else 'NULL'}, "
                    f"email = {f'\'{crmContact.contact_email}\'' if crmContact.contact_email is not None else 'NULL'}, "
                    f"phone = {f'\'{crmContact.contact_phone}\'' if crmContact.contact_phone is not None else 'NULL'}, "
                    f"mobile = {f'\'{crmContact.contact_mobile}\'' if crmContact.contact_mobile is not None else 'NULL'}, "
                    f"active = {crmContact.contact_status}, "
                    f"write_date = '{currentDateTime}', "
                    f"street3 = {f'\'{latestStreet3}\'' if latestStreet3 is not None else 'NULL'} "               
                    f"WHERE id = '{currentContactId}';"
                )
                logging.info(f"CRM Contact {index+1} Update Contact Query: {queryUpdateCrmContact}")
                curInsertOrUpdateOdooContact.execute(queryUpdateCrmContact.strip())
            else: # Insert
                
                # Get Odoo Company id
                odooActiveContactCompanyId = None
                if crmContact.company_name is not None:
                    odooActiveContactCompany = getCustomerDataFromOdoo(connOdoo, CustomerDataType.Company, CustomerDataStatus.All, company_name=crmContact.company_name)
                    if len(odooActiveContactCompany) > 0:
                        logging.info(f"Odoo Active Company After Insert {index+1}: {odooActiveContactCompany[0].display_info()}")
                        odooActiveContactCompanyId = odooActiveContactCompany[0].company_id
                
                queryInsertOdooContact = (
                    f"INSERT INTO res_partner (create_date, name, parent_id, color, create_uid, write_uid, complete_name, ref, lang, tz, function, type,"
                    f"street, street2, email, phone, mobile, commercial_company_name, active, is_company, partner_share, write_date, message_bounce, supplier_rank, "
                    f"customer_rank, invoice_warn, calendar_last_notif_ack, purchase_warn, sale_warn, street3)"
                    f"VALUES ('{currentDateTime}', "
                    f"{f'\'{crmContactName}\'' if crmContactName is not None else 'NULL'}, "
                    f"{f'\'{odooActiveContactCompanyId}\'' if odooActiveContactCompanyId is not None else 'NULL'}, "
                    f"0, 1, 1, "
                    f"{f'\'{crmContact.company_name}, {crmContactName}\'' if crmContact.company_name is not None and crmContactName is not None else 'NULL'}, "
                    f"{f"\'Update Contact at {currentDateTime.strftime("%Y-%m-%d %H:%M:%S")}: Range from {start} to {end}\'"}, "
                    f"'en_US', 'Asia/Hong_Kong',"
                    f"{f'\'{crmContact.contact_function}\'' if crmContact.contact_function is not None else 'NULL'}, "
                    f"'contact', "
                    f"{f'\'{latestStreet}\'' if latestStreet is not None else 'NULL'}, "
                    f"{f'\'{latestStreet2}\'' if latestStreet2 is not None else 'NULL'}, "
                    f"{f'\'{crmContact.contact_email}\'' if crmContact.contact_email is not None else 'NULL'}, "
                    f"{f'\'{crmContact.contact_phone}\'' if crmContact.contact_phone is not None else 'NULL'}, "
                    f"{f'\'{crmContact.contact_mobile}\'' if crmContact.contact_mobile is not None else 'NULL'}, "
                    f"{f'\'{crmContact.company_name}\'' if crmContact.company_name is not None else 'NULL'}, "
                    f"{crmContact.contact_status}, 'f', 't', "
                    f"'{currentDateTime}', 0, 0, 1, 'no-message', "
                    f"'{currentDateTime.strftime('%Y-%m-%d %H:%M:%S')}', 'no-message', 'no-message', "
                    f"{f'\'{latestStreet3}\'' if latestStreet3 is not None else 'NULL'}) "
                    f"RETURNING id;"
                )
                
                logging.info(f"CRM Active Contact {index+1} Insert Contact Query: {queryInsertOdooContact}")
                curInsertOrUpdateOdooContact.execute(queryInsertOdooContact.strip())        
                
                # Get New Contact Id
                currentContactId = curInsertOrUpdateOdooContact.fetchone()[0]
                     
            # Update Business Category
            if businessCategoryId:
                
                if businessCategoryId == 'B39B722D-FA5B-4CB7-9362-E413BACCB929':
                    return
                
                # queryDeleteBusinessCategory = f"""DELETE FROM business_category_res_partner_rel WHERE res_partner_id = {currentContactId};"""
                # logging.info(f"CRM Contact {index+1} Delete Business Category: {queryDeleteBusinessCategory}")
                # curInsertOrUpdateOdooContact.execute(queryDeleteBusinessCategory.strip())
            
                queryInsertBusinessCategory = f"INSERT INTO business_category_res_partner_rel (res_partner_id, business_category_id) SELECT {currentContactId}, (SELECT id FROM business_category WHERE uuid = '{businessCategoryId}') WHERE NOT EXISTS (SELECT 1 FROM business_category_res_partner_rel WHERE res_partner_id = {currentContactId} AND business_category_id = (SELECT id FROM business_category WHERE uuid = '{businessCategoryId}'));"
                logging.info(f"CRM Contact {index+1} Insert Business Category: {queryInsertBusinessCategory}")
                curInsertOrUpdateOdooContact.execute(queryInsertBusinessCategory.strip())
                
        connOdoo.commit()        
        
        logging.info("--- 3) End Import Contacts ---")
        
        #endregion
        
    except Exception as e:
        print(f"An error occurred: {e} ")
        traceback.print_exc()

    finally:
        if connOrderBus: connOrderBus.close()
        if connOdoo: connOdoo.close()
        if connCrm: connCrm.close()

#endregion

if __name__ == "__main__":    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(logFile, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    logging.info(f"Start Export CRM Customer Data to Odoo: {start} - {end}")
    exportCRMCustomerDataToOdoo(start, end)
    logging.info(f"End Export CRM Customer Data to Odoo {start} - {end}")