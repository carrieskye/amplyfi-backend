import json
import os

import pymongo
from bson import json_util
from dotenv import load_dotenv
from flask import Flask
from flask_cors import CORS
from flask_restful import Resource, Api, reqparse
from pymongo import MongoClient
from collections import OrderedDict

load_dotenv()

# Setup
main_client = MongoClient(os.getenv('MONGO_URL_MAIN'))
main_db = main_client.amplyfi
docs = main_db.docs

relations_client = MongoClient(os.getenv('MONGO_URL_RELATIONS'))
relations_db = relations_client['amplyfi-relations']
companies = relations_db.companies

app = Flask(__name__)
cors = CORS(app, resources={r'/api/*': {'origins': '*'}})
api = Api(app)

app.config['CORS_HEADERS'] = 'Content-Type'


class Docs(Resource):
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('limit', type=int)
        parser.add_argument('skip', type=int)
        args = parser.parse_args()
        limit = args['limit'] if args['limit'] is not None else 20
        skip = args['skip'] if args['skip'] is not None else 0
        # return json.loads(json_util.dumps(docs.find().sort('m_szYear', pymongo.DESCENDING).limit(limit).skip(skip)))
        return json.loads(json_util.dumps(docs.find().limit(limit).skip(skip)))


class Doc(Resource):
    def get(self, doc_id):
        return get_document(doc_id)


class DocsCount(Resource):
    def get(self):
        return json.loads(json_util.dumps(docs.count()))


class Companies(Resource):
    def get(self):
        return json.loads(json_util.dumps(companies.find().sort('name', 1)))


class Company(Resource):
    def get(self, company_name):
        # company = json.loads(json_util.dumps(companies.find_one({'name': company_name})))
        # company_docs = json.loads(
        #     json_util.dumps(docs.find({'m_Companies': {'$in': [company_name]}}).sort('m_szYear', pymongo.DESCENDING)))
        # company['docs'] = company_docs
        # return company
        return json.loads(json_util.dumps(companies.find_one({'name': company_name})))


class UpdateCompanies(Resource):
    def get(self):
        documents = json.loads(json_util.dumps(docs.find().limit(200)))
        for doc in documents:
            for company in doc['m_Companies']:
                doc_simple = {
                    'id': doc['_id']['$oid'],
                    'doc_id': doc['m_szDocID'],
                    'title': doc['m_szDocTitle'],
                    'year': doc['m_szYear'],
                    'location': doc['m_szGeo1'],
                    'summary': doc['m_szDocSumamry']
                }
                if json.loads(json_util.dumps(companies.find_one({'name': company}))) is None:
                    add_company_with_doc(company, doc_simple['year'], doc_simple['location'], doc_simple,
                                         doc['m_Companies'])
                else:
                    update_company_with_doc(company, doc_simple['year'], doc_simple['location'], doc_simple,
                                            doc['m_Companies'])

        return json.loads(json_util.dumps(companies.find()))


def get_document(id):
    return json.loads(json_util.dumps(docs.find_one({'m_szDocID': id})))


def add_company_with_doc(name, year, location, doc_simple, competitors):
    competitors.pop(competitors.index(name))
    company = {
        'name': name,
        'years': {year: 1} if year else {},
        'locations': {location: 1} if location else {},
        'ids': [doc_simple['id']],
        'docs': [doc_simple],
        'competitors': {company.replace('.', ''): 1 for company in competitors}
    }
    companies.insert_one(company)
    return company


def update_company_with_doc(name, year, location, doc_simple, competitors):
    company = json.loads(json_util.dumps(companies.find_one({'name': name})))
    competitors.pop(competitors.index(name))

    if doc_simple['id'] not in company['ids']:
        if year:
            company['years'][year] = company['years'][year] + 1 if year in company['years'] else 1

        if location:
            company['locations'][location] = company['locations'][location] + 1 if location in company[
                'locations'] else 1

        for competitor in competitors:
            company['competitors'][competitor] = company['competitors'][competitor] + 1 if competitor in company[
                'competitors'] else 1

        company['ids'].append(doc_simple['id'])
        company['docs'].append(doc_simple)

    companies.update_one({'name': name},
                         {'$set': {'years': company['years'], 'locations': company['locations'], 'ids': company['ids'],
                                   'docs': company['docs'], 'competitors': company['competitors']}})


api.add_resource(Docs, '/api/docs')
api.add_resource(Doc, '/api/docs/<doc_id>')
api.add_resource(DocsCount, '/api/docs-count')
api.add_resource(Company, '/api/companies/<company_name>')
api.add_resource(Companies, '/api/companies')
api.add_resource(UpdateCompanies, '/api/update/companies')

if __name__ == '__main__':
    app.run(port='5002')
