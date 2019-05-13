import json
import os

import pymongo
from bson import json_util
from dotenv import load_dotenv
from flask import Flask
from flask_cors import CORS
from flask_restful import Resource, Api, reqparse
from pymongo import MongoClient

load_dotenv()

# Setup
main_client = MongoClient(os.getenv('MONGO_URL_MAIN'))
main_db = main_client.amplyfi
docs = main_db.docs

relations_client = MongoClient(os.getenv('MONGO_URL_RELATIONS'))
relations_db = relations_client['amplyfi-relations']
companies = relations_db.companies

app = Flask(__name__)
cors = CORS(app, resources={r"/api/*": {"origins": "*"}})
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
        return json.loads(json_util.dumps(companies.find().sort("name", 1)))


class Company(Resource):
    def get(self, company_name):
        company = json.loads(json_util.dumps(companies.find_one({"name": company_name})))
        company_docs = json.loads(
            json_util.dumps(docs.find({"m_Companies": {"$in": [company_name]}}).sort('m_szYear', pymongo.DESCENDING)))
        company['docs'] = company_docs
        return company


class UpdateCompanies(Resource):
    def get(self):
        documents = json.loads(json_util.dumps(docs.find()))
        for doc in documents:
            for company in doc['m_Companies']:
                if json.loads(json_util.dumps(companies.find_one({"name": company}))) is None:
                    add_company(company, [doc['m_szDocID']], doc['m_szYear'], doc['m_szYear'])
                else:
                    update_company(company, [doc['m_szDocID']], doc['m_szYear'], doc['m_szYear'])

        return json.loads(json_util.dumps(companies.find()))


def get_document(id):
    return json.loads(json_util.dumps(docs.find_one({"m_szDocID": id})))


def add_company(name, docs, min_year, max_year):
    company = {
        "name": name,
        "docs": docs,
        "min_year": str(min_year),
        "max_year": str(max_year)
    }
    companies.insert_one(company)
    return company


def update_company(name, new_docs, min_year, max_year):
    company = json.loads(json_util.dumps(companies.find_one({"name": name})))

    for doc in new_docs:
        if doc not in company['docs']:
            company['docs'].append(doc)

    company['min_year'] = min_year if min_year < company['min_year'] else company['min_year']
    company['max_year'] = max_year if max_year > company['max_year'] else company['max_year']

    companies.update_one({'name': name},
                         {"$set": {'docs': company['docs'], 'min_year': company['min_year'],
                                   'max_year': company['max_year']}})


api.add_resource(Docs, '/api/docs')
api.add_resource(Doc, '/api/docs/<doc_id>')
api.add_resource(DocsCount, '/api/docs-count')
api.add_resource(Company, '/api/companies/<company_name>')
api.add_resource(Companies, '/api/companies')
api.add_resource(UpdateCompanies, '/api/update/companies')

if __name__ == '__main__':
    app.run(port='5002')
