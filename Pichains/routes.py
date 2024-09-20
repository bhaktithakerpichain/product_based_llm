from stat import filemode
import sys
from threading import Thread
from flask import request, jsonify, Blueprint, render_template, redirect
from flask_restplus import Resource, Api, fields
from flask_restplus import reqparse
from app.customException import CustomError, generate_error_message
from lib.bulkUtils.pmsCandidateInfo import get_all_pms_candidate_info
from lib.bulkUtils.estampBulk import process_estamp_inventory_bulk_upload
from lib.onboard import InvestorOnboarding
from lib.eStamp import Estamp
from lib.dashboard import Dashboard
from lib.auditTrail import generate_audit_trail
import pdfrw
import requests
import time
import json
import base64
import werkzeug
import os
import ast
import io
from PIL import Image
import base64
from shutil import copyfile
from bson.objectid import ObjectId
from app.models import Mongo
from config import S3_CONFIG, pms, eSign, base_url, eStamp, eStampWallet,eStampWallet, eStampInventory
from config import eStampPayment as payment_redirect_url
from datetime import datetime, timedelta
from config import tz_IST, eSign, base_path
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfinterp import resolve1
from eStampStatuscheck import eStampOnline
from eSignStatuscheck import eSignStatuscheck

from lib.emailNotifications import estamp_success, estamp_initiated, payment_successful
from lib.testPdf import extract_information
import traceback
from lib.pdftest import convertToA4, pdfFlattening
from lib.bulkUtils.batchInfo import (get_all_batch_ids, get_batch_request, 
                                     get_failed_batch_request, get_estamp_bulk_request_batch_info, 
                                     get_all_estamp_bulk_request_batch_ids)
from lib.bulkUtils.sftp import SFTPClient
from lib.initialDoc import getDoc, saveFile
from lib.webhook import initiateWebhookUpdate
from lib.custom_pages_initiate_contract import add_custom_page
from lib.get_location import is_outside_india
import jwt
from requests.auth import HTTPBasicAuth
import base64
import pytz
import urllib.request
import urllib
import re
import pandas as pd
import numpy as np
import random
import logging.config
from app.customLogger import LOGGING_CONFIG
from lib.s3Storage import s3Storage
from lib.get_location import is_outside_india
from lib.requestValidation import RequestValidator
from lib.unsuccessful_requests import update_cancelled_status

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)

mod = Blueprint('routes', __name__)


api = Api(mod,
        title='Estamp APIs',
        description='APIs for eStamp',
        doc='/estamp_documentation')

upload_parser = api.parser()
upload_parser.add_argument('file', location='files',
                           type=werkzeug.datastructures.FileStorage, required=False)

parser = reqparse.RequestParser()
parser.add_argument('data', required=False)
parser.add_argument('orgId', required=False)
parser.add_argument('checkOrder', required=False)
parser.add_argument('reminder', required=False)
parser.add_argument('reminder_duration', required=False)
parser.add_argument('reminder_expiry', required=False)
parser.add_argument('eStampRequired', required=False)
parser.add_argument('firstPartyName', required=False)
parser.add_argument('secondPartyName', required=False)
parser.add_argument('stampDutyPaidBy', required=False)
parser.add_argument('stampDutyValue', required=False)
parser.add_argument('purposeOfStampDuty', required=False)
parser.add_argument('NoOfCopies', required=False)
parser.add_argument('articleNumber', required=False)
parser.add_argument('considerationPrice', required=False)
parser.add_argument('notaryRequired', required=False)
parser.add_argument('PaymentRequired', required=False)
parser.add_argument('amount_to_pay', required=False)
parser.add_argument('payee_name', required=False)
parser.add_argument('payee_email', required=False)
parser.add_argument('payee_mobile', required=False)
parser.add_argument('branch_uuid', required=False)
parser.add_argument('branch', required=False)
parser.add_argument('return_url', required=False)
parser.add_argument('signature_expiry', required=False)
parser.add_argument('otpRequired', required=False)
parser.add_argument('custom_reference', required=False)
parser.add_argument('templateId', required=False)
parser.add_argument('template_data', required=False)
parser.add_argument('face_capture', required=False)
parser.add_argument('location_capture', required=False)
parser.add_argument("firstPartyPan", required=False)
parser.add_argument("firstPartyMobile", required=False)
parser.add_argument("secondPartyPan", required=False)
parser.add_argument("secondPartyMobile", required=False)
parser.add_argument("exact_match", required=False)
# Added batchId parameter passed when bulk uploading excels.
parser.add_argument("batchId", required=False)
parser.add_argument("workflowType", required=False)
parser.add_argument("appendTemplate", required=False)
parser.add_argument("eStampCustomLocation", required=False)
parser.add_argument("ddpi", required=False)
parser.add_argument("isKRA", required=False)
parser.add_argument("is_flatten", required=False)
parser.add_argument("case_num_on_estamp_pages_only", required=False)
parser.add_argument("customParams", required=False)

# Added parameter for Offline Estamp without signatories
parser.add_argument("without_signatories", required=False)

# Added parameters with payment functionality.
parser.add_argument("payment_expiry_days", required=False)

# Added parameter for nuvama_cc_email_customisation
parser.add_argument("cc_emails", required=False)
parser.add_argument("case_id", required=False)
parser.add_argument("primary_holder_name", required=False)
parser.add_argument("nuvama_custom_email_template", required=False)
parser.add_argument("html_email_subject_template", required=False)
parser.add_argument("html_email_body_template", required=False)

# Initiate API part 1 of 2 apis for initiating

@api.route('/onboard/initiate')
@api.expect(upload_parser)
class OnboardInitiate(Resource):
    @api.expect(parser)
    def post(self):
        logging_params={"endpoint":"OnboardInitiate"}
        try:
            results = {}
            args = upload_parser.parse_args()
            args1 = parser.parse_args()
            if (args['file'] is None) and (args1['templateId'] is None):
                return {
                    "code": 200,
                    "message": "Failed",
                    "status": {
                        "created_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                        "statusCode": 8001,
                        "statusMessage": "File key or Template Id is missing"
                    }
                }
            args1 = parser.parse_args()
            destination = base_path+"estamp_docs/uploads/"
            if args['file'] is not None:
                file_name = args['file'].filename.replace(
                    " ", "_").replace("#", "")
                # print(destination + file_name)
                logger.info(f"IO1: Destination file {destination + file_name}", extra= logging_params)
                if os.path.exists(destination + file_name):
                    expand = 1
                    while True:
                        expand += 1
                        new_file_name = file_name.split(
                            ".pdf")[0] + "(" + str(expand) + ").pdf"
                        if os.path.exists(destination + new_file_name):
                            continue
                        else:
                            file_name = new_file_name
                            break
                file = '%s%s' % (destination, file_name)
                args['file'].save(file)
            copyfile(destination+file_name, base_path +
                     "estamp_docs/unsigned_docs/" + file_name)
            dictionary = args1
            if 'data' in dictionary:
                dictionary['data'] = dictionary.get('data').replace('\\n', '').replace('\\t', '')
            else:
                return {
                    "code": 200,
                    "message": "Failed",
                    "status": {
                        "created_at": datetime.now().strftime("%d-%B-%Y %X"),
                        "statusCode": 8004,
                        "statusMessage": "data key is missing",
                        }
                    }
            dictionary['filename'] = file_name
            dictionary['uploadLocation'] = destination+file_name
            onboardInstance = InvestorOnboarding()
            logger.info(f"IO2: Calling Invest Onboard function", extra= logging_params)
            res = onboardInstance.start_onboarding(dictionary)
            logger.info(f"IO3: Final Response", extra= logging_params)
            return res
        except Exception as e:
            logger.info(f"{str(e)}", extra= logging_params)
            return {
                "code": 500,
                "message": "Failed",
                "status": {
                    "created_at": datetime.now().strftime("%d-%B-%Y %X"),
                    "statusCode": 5001,
                    "statusMessage": "Internal Server Error"
                }
            }


def selfieSaver(base64_string, filename, dimension=(120, 120)):
    s3_obj = s3Storage()
    if 'base64,' in base64_string:
        proper_format_string = base64_string.split('base64,')[1]
    else:
        proper_format_string = base64_string
    
    imagedata = base64.b64decode(proper_format_string)
    im = Image.open(io.BytesIO(imagedata))
    im1 = im.resize(dimension)
    selfie_storing_directory = base_path+"estamp_docs/recepient_selfies/"
    if not os.path.exists(selfie_storing_directory):
        os.makedirs(selfie_storing_directory)
    selfie_file = f"{selfie_storing_directory}{filename}.jpg"
    im1.save(selfie_file)

    get_s3_storage = s3_obj.save_document_s3(selfie_file, S3_CONFIG["recepient_selfies_folder"], content_type="image/jpeg")
    s3_selfies = get_s3_storage["doc_link"]

    os.remove(selfie_file)

    return s3_selfies


def fixPageNum(estamps, filename, pageNo):
    print("pageNum check")
    print(estamps, filename, pageNo)
    #f = open(filename, 'rb')
    try:
        print("Inside")
        path = base_path + "estamp_docs/unsigned_docs/" + filename
        print("path", path)
        pdf = pdfrw.PdfReader(path)
        total_pages = len(pdf.pages)
        pageCount = len(pageNo.split(","))
    except Exception as e:
        print("Issue ::: ", e)
    #f = open(base_path+'estamp_docs/unsigned_docs/' + filename, 'rb')
    #parser1 = PDFParser(f)
    #document = PDFDocument(parser1)
    #total_pages = resolve1(document.catalog['Pages'])['Count']
    #pageCount = len(pageNo.split(","))
    if pageCount == total_pages:
        newPageNo = ""
        for x in pageNo.split(","):
            newPageNo = newPageNo + "," + str(int(x)+estamps)
        # for x in range(1,total_pages+estamps+1):
        #    newPageNo = newPageNo + "," + str(x)
        return newPageNo[1:]
    else:
        newPageNo = ""
        for x in pageNo.split(","):
            newPageNo = newPageNo + "," + str(int(x)+estamps)
        return newPageNo[1:]
    return pageNo

def check_group_number(args1):
  res = json.loads(args1['data'])
  for i in res:
    if "group_number" not in res[i]:
      res[i]['group_number']=None
      
    if res[i]['group_number'] =="":
        res[i]['group_number']=None

    if "comment" not in res[i]:
      res[i]['comment']=None
        
  args1.update(data=str(res))
  return args1
# initiate_contract API single for initiating

@api.route('/onboard/initiate_contract')
@api.expect(upload_parser)
class OnboardInitiate(Resource):
    @api.expect(parser)
    def post(self):
        logging_params={"endpoint":"initiate_contract"}
        stored_uploads = False
        stored_unsigned_docs = False
        orgId = None
        try:
            start = time.time()
            s3_obj = s3Storage()
            results = {}
            args = upload_parser.parse_args()
            args1 = parser.parse_args()
            
            try:
                org_data = Mongo.find_one_internal(Mongo, {"_id": ObjectId(args1["orgId"])}, "organisations")
            except:
                raise CustomError(8003)
            
            if org_data is None:
                raise CustomError(8003)
            
            orgId = args1["orgId"]

            if "nuvama_custom_email_template" in args1 and args1["nuvama_custom_email_template"] == "true":
                if ("legal_name" not in org_data ) or ("legal_name" in org_data and org_data ["legal_name"] == None):
                    if ("extra_cc" not in org_data) or ("extra_cc" in org_data and org_data ["extra_cc"] == None):
                        org_data ["legal_name"] = "nuvama_group"
                        org_data ["extra_cc"] = "true"
                        ref = Mongo.update_one(Mongo, org_data, "organisations")

                org_data_esign = Mongo.find_one_internal_esign(Mongo, {"_id": ObjectId(args1["orgId"])}, "organisations")
                if ("legal_name" not in org_data_esign ) or ("legal_name" in org_data_esign and org_data_esign ["legal_name"] == None):
                    if ("extra_cc" not in org_data_esign) or ("extra_cc" in org_data_esign and org_data_esign ["extra_cc"] == None):
                        org_data_esign ["legal_name"] = "nuvama_group"
                        org_data_esign ["extra_cc"] = "true"
                        req = Mongo.update_one_esign(Mongo, org_data_esign,"organisations")

                org_data_aml = Mongo.find_one_internal_aml(Mongo, {"_id": ObjectId(args1["orgId"])}, "organisations")
                if ("legal_name" not in org_data_aml ) or ("legal_name" in org_data_aml and org_data_aml ["legal_name"] == None):
                    if ("extra_cc" not in org_data_aml) or ("extra_cc" in org_data_aml and org_data_aml ["extra_cc"] == None):
                        org_data_aml ["legal_name"] = "nuvama_group"
                        org_data_aml ["extra_cc"] = "true"
                        req = Mongo.update_one_aml(Mongo, org_data_aml,"organisations")

            if 'procurement_mode' in org_data and org_data['procurement_mode'] == 'OFFLINE':
                if 'eStampRequired' in args1 and args1['eStampRequired'] == 'true':
                    try:

                        url =f"{eStampInventory['url']}/api/v1/get_estamps_in_group_by_secondparty?org_id={orgId}"
                        headers = {
                            'Content-Type': 'application/json',
                            "apikey": eStampInventory['apikey']
                            }
                        available_estamps = requests.get(url, headers=headers)
                        
                        region_data = available_estamps.json()["data"][args1["branch"]]

                        stamps_needed = 0
                        stamp_duty_value = 0
                        # stammp_duty_list = [int(value) for value in args1["stampDutyValue"].split(',')]
                        if ',' in args1["stampDutyValue"]:
                            # Split the string by commas and convert to a list of integers
                            stammp_duty_list = [int(value) for value in args1["stampDutyValue"].split(',')]
                        else:
                            # Convert the single value to an integer and wrap it in a list
                            stammp_duty_list = [int(args1["stampDutyValue"])]

                        # Create an empty dictionary to store occurrences
                        occurrence_dict = {}
                        if len(stammp_duty_list) >= 1:
                            for value in stammp_duty_list:
                                if value in occurrence_dict:
                                    occurrence_dict[value] += 1
                                else:
                                    occurrence_dict[value] = 1
                            # stamps_needed = len(stammp_duty_list)

                            for key,value in occurrence_dict.items():
                                fetched_count = region_data.get(str(float(key)))[args1["firstPartyName"]]
                                fetched_value = list(fetched_count.values())[0]
                                if fetched_value < value:
                                    raise CustomError(8028)
                                if 'exact_match' in args1 and args1['exact_match'] == 'true':
                                    if args1["secondPartyName"] not in fetched_count:
                                        raise CustomError(8028)
                                    elif args1["secondPartyName"] in fetched_count and fetched_count.get(args1["secondPartyName"]) < value:
                                        raise CustomError(8028)

                    except:
                        raise CustomError(8028)


            if 'without_signatories' in args1 and args1['without_signatories'] is None:
                args1['without_signatories'] = 'false'

            print(args1)
            #checking without_signatories is enabled for the organisations.
            # if (('without_signatories' in org_data) and (org_data['without_signatories'] == "true")):
            if (('without_signatories' in args1) and (args1['without_signatories'] == "true")):
                if "eStampRequired" in args1 and args1["eStampRequired"]!="true":
                    raise CustomError(8039)
            #grouping mechanism
            if (('enable_grouping' in org_data) and (org_data['enable_grouping'] == "true")):
                new_args1=check_group_number(args1)
                args1=new_args1
            append = args1.get('appendTemplate', None)

            if (args['file'] is None) and (args1['templateId'] is None):
                return {
                    "code": 200,
                    "message": "Failed",
                    "status": {
                        "created_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                        "statusCode": 8001,
                        "statusMessage": "File key or Template Id is missing"
                    }
                }
    
            # print(args1)
            logger.info(f"IC1, uploaded args {args1}", extra=logging_params)
            destination = base_path+"estamp_docs/uploads/"
            # New destination is s3 folder + uploads
            if args1["location_capture"] is not None:
                args1['location_capture'] =str(args1.get('location_capture',"false").lower())
            if args1["face_capture"] is not None:
                args1['face_capture'] =str(args1.get('face_capture',"false").lower())
            # print(args1)
            logger.info(f"IC2, uploaded args2 {args1}", extra=logging_params)
            file_name = getDoc(file_args= args, template_args= args1, 
                                    destination= destination, append = append)
            stored_uploads = True

            # New destination is s3 folder + unsigned_docs + filename
            onboard_input_file = base_path + "estamp_docs/unsigned_docs/" + file_name

            #adding custom_page
            #if custom_page_exist==True:
            custom_pages = Mongo.find_one_internal(Mongo, {"_id": ObjectId(args1["orgId"])}, "organisations")
            custom_page=custom_pages.get("custom_page")
            args1['custom_page']=custom_page
            
            if args1["custom_page"] is not None:
                logger.info("CUSTOM PAGE IS NOT NONE")
                
                custom_page_=add_custom_page(args1,destination,file_name,onboard_input_file)

            else:
                logger.info("CUSTOM PAGE IS NONE")        
            copyfile(destination+file_name, onboard_input_file)
            
            # Save to S3 bucket/uploads
            try:
                t0 = time.time()
                get_s3_storage = s3_obj.save_document_s3(destination+file_name, S3_CONFIG["uploads_folder"])
                t1 = time.time()
                logger.info(f"ICPERF1, TOTAL TIME TAKEN TO UPLOAD TO S3 {t1-t0}", extra=logging_params)
                s3_uploads_url = get_s3_storage["doc_link"]
                stored_unsigned_docs = True
            except Exception as e:
                raise CustomError(8050)
            
            try:
                # KPMG - Removing Entity ID present in Custom Reference
                admin = Mongo.find_one_internal(Mongo, {'_id': ObjectId(args1['orgId'])}, "organisations")
                if re.search("kpmg", admin['name'], flags = re.I):
                    args1['custom_reference'] = args1['custom_reference'].split("||")[0]
            except:
                pass

            dictionary = args1

            # Removed checkOrder === true as default
            # dictionary['checkOrder'] = "true"
            
            dictionary['filename'] = file_name
            # Upload location in S3
            dictionary['uploadLocation'] = destination+file_name
            onboardInstance = InvestorOnboarding()
            dictionary['init_api'] = True
            dictionary['init_api_new'] = True

            if 'procurement_mode' in admin and admin['procurement_mode'] == 'ONLINE':
                dictionary['stampDutyPaidBy'] = 'first_party_name' if dictionary['stampDutyPaidBy'] == dictionary['firstPartyName'] else 'second_party_name'

            logger.info(f"IC3, Onboarding started!!!", extra=logging_params)
            res = onboardInstance.start_onboarding(dictionary)
            admin = Mongo.find_one_internal(
                Mongo, {'_id': ObjectId(dictionary['orgId'])}, "organisations")
            logger.info(f"IC4,  RES:::: {res}", extra=logging_params)

            t0 = time.time()
            try:
                if res['code'] != 400:
                    if dictionary['eStampRequired'] == "true":
                        if (admin["procurement_mode"] == "ONLINE") or (dictionary['PaymentRequired'] == "true"):
                            try:
                                dictionary['data'] = ast.literal_eval(
                                    dictionary['data'])
                            except:
                                dictionary['data']=json.loads(dictionary['data'])
                    print("before")
                    logger.info(f"IC5, before!!!", extra=logging_params)
                    for d in res['requests']:
                        logger.info(f"IC6, Inside!!!", extra=logging_params)
                        total_estamps = 1
                        request = Mongo.find_one_internal(
                            Mongo, {'_id': ObjectId(d['_id'])}, 'esign')
                        # print(request)
                        logger.info(f"IC3, Request:::{request}!!!", extra=logging_params)
                        refId = request['refId']
                        orgId = request['orgId']
                        if "multiEstamp" in request:
                            total_estamps = request['totalEstamps']
                        if dictionary['eStampRequired'] == "true":
                            observer_count = 0
                            for recipient in dictionary['data']:
                                print(dictionary['data'][recipient])
                                if dictionary['data'][recipient]['observer'] == "true":
                                    observer_count = observer_count+1
                                    continue
                                if int(recipient.split("recipient")[1]) == request['order']+observer_count:
                                    # Signature location will be updated during estamp status check
                                    request['rectangle'] = dictionary['data'][recipient]['rectangle']
                                    request['pageNo'] = dictionary['data'][recipient]['pageNo']
                                    
                                    # Removed signature location update
                                    # request['pageNo'] = fixPageNum(
                                    #     total_estamps, file_name, dictionary['data'][recipient]['pageNo'])
                                    # print("+++++++++++++", dictionary['data'][recipient]['rectangle'])
                                    # if ";" in dictionary['data'][recipient]['rectangle']:
                                    #     for stamp in reversed(range(1,total_estamps+1)):
                                    #         request['rectangle'] = request['rectangle'].split(
                                    #             ";")[0] + ";" + request['rectangle']
                                    #         request['pageNo'] = str(stamp)+","+request['pageNo']
                                    
                                    request['reason'] = dictionary['data'][recipient]['reason']
                                    request['location'] = dictionary['data'][recipient]['location']
                            ref = Mongo.update_one(Mongo, request, 'esign')
                    if dictionary["draftId"] is not None:
                        draft = Mongo.find_one_internal(
                            Mongo, {"_id": ObjectId(dictionary['draftId'])}, "drafts")
                        reference = Mongo.find_one_internal(
                            Mongo, {"orgId": orgId, "refId": refId}, "reference")
                        reference['draftId'] = dictionary['draftId']
                        print("\n\n\n\n\nReference Collection: !!!!!!!!\n\n\n\n",reference)
                        reference = Mongo.update_one(Mongo, reference, "reference")
                if dictionary["draftId"] is not None:
                    print(dictionary['draftId'])
                    #refId = request['refId']
                    #orgId = request['orgId']
                    draft = Mongo.find_one_internal(
                        Mongo, {"_id": ObjectId(dictionary['draftId'])}, "drafts")
                    draft['isDeleted'] = True
                    draft = Mongo.update_one(Mongo, draft, "drafts")
                    #reference = Mongo.find_one_internal(Mongo, {"orgId": orgId, "refId": refId},"reference")
                    #reference['draftId'] = dictionary['draftId']
                    #reference = Mongo.update_one(Mongo, reference,"reference")
            except:
                CustomError(8058)
            t1 = time.time()
            logger.info(f"ICPERF7, IC7### REFID-{refId} >> {t1-t0}", extra=logging_params)

            logger.info(f"IC7, TIME FOR COMPLETE API WITH REFID-{refId} >> {time.time()-start}", extra=logging_params)

            #Sending webhook notifications via Threads
            t0 = time.time()
            try:
                thread = Thread(target=initiateWebhookUpdate, args=[refId, orgId])
                thread.start()
                logger.info(f"IC8, REFID : {refId} INITIATED INITIATE CONTRACT WEBHOOK THREAD AT TIME : {datetime.now()}", extra=logging_params)
            except Exception as e:
                error_msg = generate_error_message(e, sys.exc_info())
                logger.error(f"IC9, REFID : {refId} EXCEPTION INITIATE CONTRACT WEBHOOK : {error_msg}", extra=logging_params)
            t1 = time.time()
            logger.info(f"ICPERF9, WEBHOOK TIME FOR REFID-{refId} >> {t1-t0}", extra=logging_params)

            t0 = time.time()
            get_s3_storage = s3_obj.save_document_s3(onboard_input_file, S3_CONFIG["unsigned_docs_folder"])
            t1 = time.time()
            logger.info(f"ICPERF10, IC10, TIME FOR SAVING UNSIGNED DOCS TO S3 FOR REFID-{refId} >> {t1-t0}", extra=logging_params)

            t0 = time.time()
            # Remove uploads and unsigned_docs from local
            if os.path.exists(destination+file_name):
                os.remove(destination+file_name)
            if os.path.exists(onboard_input_file):
                os.remove(onboard_input_file)
          
            res = s3Storage().get_presigned_urls_initiate_contract_response(res)
            #checking without_signatories is enabled for the organisations.
            # if (('without_signatories' in org_data) and (org_data['without_signatories'] == "true")):
            if ('procurement_mode' in admin) and (admin["procurement_mode"] == "OFFLINE"):
                if (('without_signatories' in args1) and (args1['without_signatories'] == "true")):
                    reference = Mongo.find_one_internal(Mongo, {"orgId": orgId, "refId": refId},"reference")
                    reference['status'] = "COMPLETED"
                    reference = Mongo.update_one(Mongo, reference,"reference")
                    for reqs in res['requests']:
                        reqs['status'] = "COMPLETED"
                    res=fields_to_remove_without_signotaries(res)
            t1 = time.time()
            logger.info(f"ICPERF11, IC11, TIME FOR COMPLETE API WITH REFID-{refId} >> {t1-t0}", extra=logging_params)

            return res
        
        except CustomError as custom_err:
            error_message = generate_error_message(custom_err, sys.exc_info())
            # Remove uploads and unsigned_docs from local
            if stored_uploads == True and os.path.exists(destination+file_name):
                os.remove(destination+file_name)
            if stored_unsigned_docs == True and os.path.exists(onboard_input_file):
                os.remove(onboard_input_file)
            
            logger.error(f"IC10, CUSTOM ERROR : {error_message}", extra=logging_params)
            response = {
                "code": 200,
                "message": "Failed",
                "status": {
                    "created_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                    "statusCode": custom_err.error_code,
                    "statusMessage": custom_err.msg,
                }
            }

            if custom_err.refId is not None:
                refId = custom_err.refId
                response['refId'] = refId
                payload = {
                    "refId": refId,
                    "orgId": orgId,
                    "remark": custom_err.msg,
                }
                # Update cancelled status in a thread for unsuccessful requests
                thread = Thread(target=update_cancelled_status, args=[payload])
                thread.start()

                logger.info(f"REFID : {refId} INITIATED FAILED REQUEST THREAD AT TIME : {datetime.now()}", extra=logging_params)

            return response


        except Exception as e:
            error_message = generate_error_message(e, sys.exc_info())
            # Remove uploads and unsigned_docs from local
            if stored_uploads == True and os.path.exists(destination+file_name):
                os.remove(destination+file_name)
            if stored_unsigned_docs == True and os.path.exists(onboard_input_file):
                os.remove(onboard_input_file)

            logger.error(f"IC10, ERROR : {error_message}", extra=logging_params)
            response = {
                "code": 500,
                "message": "Failed",
                "status": {
                    "created_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                    "statusCode": 5001,
                    "statusMessage": "Internal Server Error"
                }
            }
            return response


def fields_to_remove_without_signotaries(res):
    # Remove the specified fields using pop()
    fields_to_remove = ["comment", "face_capture", "group_number", "isKRA", "location_capture"]
    for request in res["requests"]:
        for field in fields_to_remove:
            request.pop(field, None)         
    res.pop("sign_urls", None)
    return res

# save API single for initiating

@api.route('/onboard/save/<option>')
@api.expect(upload_parser)
class Save(Resource):
    @api.expect(parser)
    def post(self, option):
        try:
            if option not in ['esign', 'estamp']:
                raise CustomError(8037)
            
            args = upload_parser.parse_args()
            args1 = parser.parse_args()
            if args['file'] is not None:
                destination = base_path+"estamp_docs/drafts/"
                file_name = args['file'].filename.replace(" ", "_").replace("#", "")
                file_name = file_name.split(".pdf")[0] + datetime.now(tz_IST).strftime("%d-%B-%Y_%X") + ".pdf"
                file_path = destination+file_name
                args['file'].save(file_path)

                # Save to S3 bucket inside drafts
                get_s3_storage = s3Storage().save_document_s3(file_path, S3_CONFIG["drafts_folder"])
                s3_uploads_url = get_s3_storage["doc_link"]
                
                args1['draftUrl'] = s3_uploads_url
                args1['filename'] = file_name
                args1['timestamp'] = datetime.now(tz_IST).strftime("%d-%B-%Y %X")

                # cleanup file from local storage
                if os.path.exists(file_path):
                    os.remove(file_path)

            if option == "estamp":
                onboardInstance = InvestorOnboarding()
                res = onboardInstance.saveEstamp(args1)
                return res
            elif option == "esign":
                onboardInstance = InvestorOnboarding()
                res = onboardInstance.saveEsign(args1)
                return res

        except CustomError as custom_err:
            return {
                "code": 200,
                "message": "Failed",
                "status": {
                    "processed_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                    "statusCode": custom_err.error_code,
                    "statusMessage": custom_err.msg
                }
            }
        
        except Exception as e:
            return {
                "code": 500,
                "message": "Failed",
                "status": {
                    "processed_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                    "statusCode": 5001,
                    "statusMessage": "Internal Server Error"
                }
            }


parser.add_argument('draftId', required=False)


@api.route('/onboard/drafts/<orgId>')
class OnboardDashboard(Resource):
    def get(self, orgId):
        dash = Dashboard()
        res = dash.drafts(orgId)
        return jsonify(res)


@api.route('/onboard/drafts/<orgId>/<draftId>')
class OnboardDashboard(Resource):
    def get(self, orgId, draftId):
        dash = Dashboard()
        res = dash.draft(orgId, draftId)
        return jsonify(res)


@api.route('/onboard/checkDocument')
class checkDocument(Resource):
    def post(self):
        args = upload_parser.parse_args()
        source_file = base_path+'estamp_docs/document_check/checkDocument_'+str(datetime.now())+ '_.pdf'
        args['file'].save(source_file)
        res = extract_information(source_file)
        if os.path.exists(source_file):
            os.remove(source_file)
        return jsonify(res)


parser.add_argument('rectangle', required=False)
parser.add_argument('pageNo', required=False)
parser.add_argument('reason', required=False)
parser.add_argument('location', required=False)
parser.add_argument('selfie', required=False)

# Init API single for initiating


@api.route('/onboard/_init')
@api.expect(upload_parser)
class OnboardInitiate(Resource):
    @api.expect(parser)
    def post(self):
        try:
            results = {}
            args = upload_parser.parse_args()
            if args['file'] is None:
                return {
                    "code": 200,
                    "message": "Failed",
                    "status": {
                        "created_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                        "statusCode": 8001,
                        "statusMessage": "File key is missing"
                    }
                }
            args1 = parser.parse_args()
            destination = base_path+"estamp_docs/uploads/"
            if args['file'] is not None:
                file_name = args['file'].filename.replace(
                    " ", "_").replace("#", "")
                print(destination + file_name)
                if os.path.exists(destination + file_name):
                    expand = 1
                    while True:
                        expand += 1
                        new_file_name = file_name.split(
                            ".pdf")[0] + "(" + str(expand) + ").pdf"
                        if os.path.exists(destination + new_file_name):
                            continue
                        else:
                            file_name = new_file_name
                            break
                file = '%s%s' % (destination, file_name)
                args['file'].save(file)
            copyfile(destination+file_name, base_path +
                     "estamp_docs/unsigned_docs/" + file_name)
            dictionary = args1
            dictionary['filename'] = file_name
            dictionary['uploadLocation'] = destination+file_name
            onboardInstance = InvestorOnboarding()
            dictionary['init_api'] = True
            res = onboardInstance.start_onboarding(dictionary)
            admin = Mongo.find_one_internal(
                Mongo, {'_id': ObjectId(dictionary['orgId'])}, "organisations")
            print("res", res)
            for d in res['requests']:
                total_estamps = 1
                request = Mongo.find_one_internal(
                    Mongo, {'_id': ObjectId(d['_id'])}, 'esign')
                print(request)
                if "multiEstamp" in request:
                    total_estamps = request['totalEstamps'] - 1
                request['rectangle'] = dictionary['rectangle']
                request['pageNo'] = fixPageNum(
                    total_estamps, file_name, dictionary['pageNo'])
                request['reason'] = dictionary['reason']
                request['location'] = dictionary['location']
                ref = Mongo.update_one(Mongo, request, 'esign')

            return res
        except Exception as e:
            print(e)
            return {
                "code": 500,
                "message": "Failed",
                "status": {
                    "created_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                    "statusCode": 5001,
                    "statusMessage": "Internal Server Error"
                }
            }


@api.route('/onboard/reference/<orgId>/<refId>')
class OnboardDashboard(Resource):
    def get(self, orgId, refId):
        dash = Dashboard()
        res = dash.reference(orgId, refId)
        try:
            for esign in res["eSign"]:
                if "downloadUrl" not in esign:
                    # esign["downloadUrl"] = base_url + "docs/signed_docs/"+res.get('filename', '')
                    esign["downloadUrl"] = esign['uploadUrl']
        except Exception as e:
            print("not present!!!!")
        return jsonify(res)

# New Dashboard below


"""@api.route('/onboard/dashboard/<orgId>/<limit>/<offset>')
class OnboardDashboard(Resource):
    def get(self, orgId, limit, offset):
        dash = Dashboard()
        res = dash.auditTrail(orgId, limit, offset)
        return jsonify(res)
"""
@api.route("/onboard/dashboard/<orgId>/<limit>/<offset>")
class OnboardDashboard(Resource):
    def get(self, orgId, limit, offset):
        dash = Dashboard()
        status  = request.args.get('status')
        if status:
            status_sent = status
        else:
            status_sent = None


        refId = request.args.get('refId')
        if refId:
            refId_sent = refId
        else:
            refId_sent = None

        email = request.args.get('email')
        if email:
            email_sent = email
        else:
            email_sent = None
        print("emaialllll",email_sent)

        custom_reference = request.args.get('custom_reference')
        if custom_reference:
            custom_reference_sent = custom_reference
        else:
            custom_reference_sent = None
        print("custom_reference",custom_reference)
        print("status",status)


        phoneNumber = request.args.get('phoneNumber')
        print("phoneNumberfirsttttt",phoneNumber)
        if phoneNumber:
            phoneNumber_sent = phoneNumber
        else:
            phoneNumber_sent = None
        print("phoneNumber",phoneNumber)
        
        
        
        start_date = request.args.get('start_date')
        
        end_date = request.args.get('end_date')
        print("start_date",start_date)
        print("start_date",end_date)
        if start_date:
            start_date_sent = start_date
            end_date_sent = end_date
        elif end_date:
            start_date_sent = start_date
            end_date_sent = end_date
        else:
            start_date_sent = None
            end_date_sent = None

        res = dash.auditTrail(orgId, limit, offset,status_sent,refId_sent,email_sent,custom_reference_sent,phoneNumber_sent,start_date_sent,end_date_sent)
        return jsonify(res)


# old dashboard api
# @api.route('/onboard/dashboard/<orgId>')
# class OnboardDashboard(Resource):
#    def get(self, orgId):
#        dash = Dashboard()
#        res = dash.auditTrail(orgId)
#        return jsonify(res)


Process = api.model("Process", {
    'data': fields.String('534278'),
    'orgId': fields.String('534278')
})


@api.route('/onboard/process')
class Process(Resource):
    @api.expect(Process)
    def post(self):
        data = api.payload['data']
        orgId = api.payload['orgId']
        if len(data) < 1:
            return {"code": 400, "message": "no esign found"}
        for d in data:
            request = Mongo.find_one_internal(
                Mongo, {'_id': ObjectId(d['_id'])}, 'esign')
            request['rectangle'] = d['rectangle']
            total_estamps = 1
            print("request", request)
            if "multiEstamp" in request:
                total_estamps = request['totalEstamps']
            request['pageNo'] = d['pageNo']
            request['pageNo'] = d['pageNo']
            request['reason'] = d['reason']
            request['location'] = d['location']
            ref = Mongo.update_one(Mongo, request, 'esign')
        esigns = Mongo.find_cond(
            Mongo, {"refId": data[0]['refId'], "orgId": data[0]['orgId']}, "esign")
        if len(esigns) < 1:
            return {"code": 400, "message": "no esign found"}
        if 'eStampId' not in esigns[0]:
            payload = {
                'data': data,
                'orgId': orgId
            }
            headers = {
                'Content-Type': 'application/json',
                'apikey': eSign['apikey']
            }
            resp = requests.post(
                eSign['url'] + "/v1/process", data=json.dumps(payload), headers=headers)
            for r in esigns:
                r['status'] = "in-progress"
                res = Mongo.update_one(Mongo, r, "esign")
                audit_trail = {}
                audit_trail['refId'] = r['refId']
                audit_trail['orgId'] = r['orgId']
                audit_trail['step'] = "eSign"
                audit_trail['user'] = r['name']
                audit_trail['order'] = r['order']
                audit_trail['status'] = "in-progress"
                audit_trail['docUrl'] = r['uploadUrl']
                audit_trail['datetime'] = datetime.now(
                    tz_IST).strftime("%d-%B-%Y %X")
                ref = Mongo.insert_one(Mongo, audit_trail, "audit_trail")
        return ({
                "code": 200,
                "message": "success"
                })


@api.route('/estamp/branch/<orgId>')
class Branch(Resource):
    def get(self, orgId):
        try:
            estamp = Estamp()
            admin = Mongo.find_one_internal(
                Mongo, {'_id': ObjectId(orgId)}, "organisations")
            if admin["procurement_mode"] == "OFFLINE":
                branches = Mongo.find_one_internal(Mongo, {"type":"OFFLINE"},"branch")
                branch_list = []
                for key, value in branches["branches"].items():
                    branch = {}
                    branch['branch_uuid'] = value
                    branch['name'] = key
                    branch_list.append(branch)
                # print(branch_list)
                logger.info(f"ORG ID : {orgId}, ESTAMP BRANCHES OFFLINE : {branch_list}")
                return branch_list
            
            elif admin["procurement_mode"] == "ONLINE":
                if "auth_type" in admin and admin['auth_type'] == "test":
                    url = eStamp["uat_url"]
                    if ("api_key" in admin) and ("secret_key" in admin):
                        logger.info(f"ORG ID : {orgId}, GETTING DOQFY BRANCHES, ESTAMP ONLINE AUTH TYPE == TEST")
                        branch_list = estamp.getBranch(admin['api_key'], admin['secret_key'], url)
                    else:
                        branch_list = estamp.getBranch(url=url)
                else:
                    if "api_key" in admin:
                        logger.info(f"ORG ID : {orgId}, GETTING DOQFY BRANCHES, ESTAMP ONLINE USING ADMIN DETAILS")
                        branch_list = estamp.getBranch(admin['api_key'], admin['secret_key'])
                    else:
                        logger.info(f"ORG ID : {orgId}, GETTING DOQFY BRANCHES, ESTAMP ONLINE USING CONFIG DETAILS")
                        branch_list = estamp.getBranch()
                
                logger.info(f"ORG ID : {orgId}, ESTAMP BRANCHES DOQFY : {branch_list}")
                return branch_list

            elif "esbtr" in admin:
                branches = []
                branches_esbtr = Mongo.find_one_internal(Mongo, {"type":"OFFLINE-ESBTR"},"branch")
                for key, value in branches_esbtr["branches"].items():
                    branch = {}
                    branch['branch_uuid'] = value
                    branch['name'] = key
                    branch['type'] = "esbtr"
                    branches.append(branch)
            # logger.info(f"ORG ID : {orgId}, ESTAMP BRANCHES : {branches}")
            return branches
        except Exception as err:
            print(str(err))
            return {
                "code": 500,
                "message": "Failed",
                "status": {
                    "created_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                    "statusCode": 5001,
                    "statusMessage": "Internal Server Error"
                }
            }


SignerDetails = api.model("SignerDetails", {
    'IP': fields.String('534278'),
    'geoLocation': fields.String('534278'),
    'userAgent': fields.String('534278'),
    'selfie': fields.String('534278')

})

@api.route('/signer_details/<orgId>/<refId>/<order>')
class SignerDetails(Resource):
    def post(self, orgId, refId, order):
        request = Mongo.find_one_internal(Mongo, data={"orgId": orgId, "refId": int(
            refId), "order": int(order)}, coll_name="esign")
        audit_trail = {}
        audit_trail['refId'] = int(refId)
        audit_trail['orgId'] = orgId
        audit_trail['step'] = "eSign"
        print(request)
        try: 
            audit_trail['user'] = request['name']
        except:
            audit_trail['user'] = ''
        try:
            audit_trail['order'] = request['order']
        except: 
            audit_trail['order'] = ''
        try:
            audit_trail['docUrl'] = request.get('uploadUrl', '')
        except:
            audit_trail['docUrl'] = ''
        audit_trail['datetime'] = datetime.now().strftime("%d-%B-%Y %X")
        try:
            audit_trail['status'] = api.payload['status']
        except:
            audit_trail['status'] = ''
        if "IP" in api.payload:
            audit_trail['IP'] = api.payload['IP']
        if "userAgent" in api.payload:
            audit_trail['OS'] = api.payload['userAgent']['OS']
            audit_trail['browser'] = api.payload['userAgent']['Browser']
        if "geoLocation" in api.payload:
            if bool(api.payload['geoLocation']):
                audit_trail['lat'] = api.payload['geoLocation']['lat']
                audit_trail['long'] = api.payload['geoLocation']['long']
        if "selfie" in api.payload:
            base64_image = api.payload['selfie']
            # filename = selfieSaver(base64_image, filename=request['name'])
            # audit_trail['selfie'] = f'{eSign["url"]}/estamp_docs/recepient_selfies/{filename}.jpg'
            s3_selfie_link = selfieSaver(base64_image, filename=request['name'])
            audit_trail['selfie'] = s3_selfie_link

        ref = Mongo.insert_one(Mongo, audit_trail, "audit_trail")
        generate_audit_trail(orgId, str(refId))
        return {
            "code": 200,
            "message": "Success",
            "status": {
                "created_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                "statusCode": 200,
                "statusMessage": "Signer Details added"
            }
        }


@api.route('/audit_trail/<orgId>/<refId>')
class AuditTrail(Resource):
    def get(self, orgId, refId):
        generate_audit_trail(orgId, str(refId))
        #return render_template('auditTrail.html', documentHash="test")
        return {
            "code": 200,
            "message": "Success",
            "status": {
                "created_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                "statusCode": 200,
                "statusMessage": "Signer Details added"
            }
        }


@api.route('/estamp/articles/<orgId>/<branch>')
class Articles(Resource):
    def get(self, orgId, branch):
        admin = Mongo.find_one_internal(
                Mongo, {'_id': ObjectId(orgId)}, "organisations")
        if admin["procurement_mode"] == "OFFLINE":
            branch = branch.replace("_", " ")
            print("x", branch, "x")
            articles = Mongo.find_one_internal(
                Mongo, {"branch": branch}, "articles")
            if articles is None:
                return ({
                    "articles": []
                })
            print(articles['branch'], articles['articles'])
            return ({
                "articles": articles['articles']
            })
        else:
            eStampInstance = Estamp()
            if 'auth_type' in admin and admin['auth_type'] == 'test':
                url = eStamp['uat_url']
                if 'api_key' in admin:
                    resp = eStampInstance.getArticle(branch, admin['api_key'], admin['secret_key'], url)
                else:
                    resp = eStampInstance.getArticle(branch, url=url)
            else:
                if 'api_key' in admin:
                    resp = eStampInstance.getArticle(branch, admin['api_key'], admin['secret_key'])
                else:
                    resp = eStampInstance.getArticle(branch)

            return ({"articles": resp})


@api.route('/estamp/setWebhook/<orgId>')
class Webhook(Resource):
    def post(self, orgId):
        subscribeUrl = request.form.get('subscribeUrl')
        headers = request.form.get('headers')
        try:
            headers = ast.literal_eval(headers)
        except:
            pass
        webhook = Mongo.find_one_internal(Mongo, {"orgId": orgId}, "webhooks")
        # print(webhook)
        print(f"ORG ID : {orgId}, SUBSCRIBE URL: {subscribeUrl}, HEADERS : {headers}", flush=True)
        if webhook is None:
            # save this url in database for a given orgId, refId
            insert_data = {
                "orgId": orgId,
                "subscribeUrl": subscribeUrl,
                "headers" : headers
            }
            webhookId = Mongo.insert_one(Mongo, insert_data, 'webhooks')
            if webhookId is not None:
                return ({
                    "code": 200,
                    "message": "Webhook set successfully"
                })
            else:
                return ({
                    "code": 500,
                    "message": "Webhook request failed"
                })
        else:
            webhook['subscribeUrl'] = subscribeUrl
            webhook["headers"] = headers
            webhookId = Mongo.update_one(Mongo, webhook, 'webhooks')
            if webhookId is not None:
                return ({
                    "code": 200,
                    "message": "Webhook set successfully"
                })
            else:
                return ({
                    "code": 500,
                    "message": "Webhook request failed"
                })


@api.route('/estamp/status_update_new')
class eStampStatus(Resource):
    def get(self):
        eStampStatuscheck()
        return


@api.route('/esign/status_update_new')
class eSignStatus(Resource):
    def get(self):
        eSignStatuscheck()
        return


payment_parser = reqparse.RequestParser()
payment_parser.add_argument('razorpay_payment_link_status', required=False)
payment_parser.add_argument('razorpay_payment_link_id', required=False)


@api.route('/estamp/payment_updates')
class eStampPayment(Resource):
    @api.expect(payment_parser)
    def get(self):
        logging_params={"endpoint":"payment_updates callback"}
        try:
            args = payment_parser.parse_args()
            print(args)
            logger.info(f"PU1-- initiating estamp attching process",extra=logging_params)
            if args['razorpay_payment_link_status'] == "paid":
                logger.info(f"PU2-- payment is done for payment Id: {args['razorpay_payment_link_id']} with status: {args['razorpay_payment_link_status']}",extra=logging_params)
                if args['razorpay_payment_link_id'] is not None:
                    # Find by refId in payments_requests collection
                    payments_request = Mongo.find_one_internal(
                        Mongo, {"payment_id": args['razorpay_payment_link_id']}, "payments_requests")

                    if payments_request["status"] != "paid":

                        payments_request['payment_status'] = args['razorpay_payment_link_status']
                        payments_request['status'] = args['razorpay_payment_link_status']
                        logger.info(f"PU3-- update estamp collection : {eStamp} for payment Id: {args['razorpay_payment_link_id']} with status: {args['razorpay_payment_link_status']}",extra=logging_params)
                        payments_request_updated = Mongo.update_one(Mongo, payments_request, 'payments_requests')
                        # print(eStamp)
                        refId = payments_request['refId']

                        # 
                        # reference = Mongo.find_one_internal(
                        #     Mongo, {"estamp":{"$elemMatch": {"payment_id": str(args['razorpay_payment_link_id'])}}}, "reference")
                        reference = Mongo.find_one_internal(Mongo, {"refId" : int(refId)}, "reference")

                        reference_id = reference['_id']
                        # reference['status'] = "IN_PROGRESS"
                        # # reference["estamp"][0]["status"] = "in_progress"
                        # # reference["estamp"][0]["payment_status"] = args['razorpay_payment_link_status']

                        # for i in reference["eSign"]:
                        #     i["status"] = "in-progress"
                        # logger.info(f"PU4-- update reference collection : {reference} for payment Id: {args['razorpay_payment_link_id']} with status: {args['razorpay_payment_link_status']}",extra=logging_params)
                        # ref = Mongo.update_one(Mongo, reference, "reference")
                        # reference['_id'] = reference_id
                        admin = Mongo.find_one_internal(
                            Mongo, {'_id': ObjectId(payments_request['orgId'])}, "organisations")

                        # send succes
                        logger.info(f"PU5-- Sending Success Mail for payment Id: {args['razorpay_payment_link_id']} with status: {args['razorpay_payment_link_status']}",extra=logging_params)
                        # ref = Mongo.find_one_internal(
                        #     Mongo, {"estamp":{"$elemMatch": {"payment_id": str(args['razorpay_payment_link_id'])}}}, "reference")
                        to_email = [admin['email'], payments_request['payee_email']]
                        res = payment_successful(payments_request['orgId'], reference['filename'], admin['email'], 
                                    payments_request['payment_details']['reference_id'], to_email)
                        logger.info(f"PU5.1-- Success Mail sent for payment Id: {args['razorpay_payment_link_id']} with result: {res}",extra=logging_params)

                        if res["code"] == '200':
                            init = InvestorOnboarding()
                            s3_storage_obj = s3Storage()

                            filename = reference["eSign"][0]["filename"]
                            path = base_path + "estamp_docs/unsigned_docs/" + filename
                            upload_destination = os.path.join(base_path, "estamp_docs/uploads/", filename)
                            
                            # destination_path = None
                            # if not os.path.exists(path):
                            # Fetch unsigned docs
                            file_to_be_downloaded = os.path.join(S3_CONFIG["unsigned_docs_folder"], filename)
                            destination_path = os.path.join(base_path, "estamp_docs/unsigned_docs/", filename)
                            s3_storage_obj.download_s3_file(file_to_be_downloaded, destination_path)
                            path = destination_path

                            if not os.path.exists(upload_destination):
                                # Fetch initial upload file
                                initial_uploaded_file = os.path.join(S3_CONFIG["uploads_folder"], filename)
                                s3_storage_obj.download_s3_file(initial_uploaded_file, upload_destination)

                            cached_details = Mongo.find_one_internal(Mongo, {"refId": reference["refId"]}, "payment_cache")
                            cached_data = cached_details['data']
                            cached_data["PaymentRequired"] = 'false'

                            esign_response = cached_details['esign_response']
                            logger.info(f"PU6-- REF ID : {reference['refId']} - initiating estamp generation",extra=logging_params)
                            eStamp_data, eStampId, reference, file_url = init.estamp_generation(cached_data, 
                                                admin, refId, esign_response, reference)
                            print("REFERENCE AFTER ESTAMP GENERATION : ", reference)
                            
                            return_response = init.process_further(eStamp_data, eStampId, reference, file_url, cached_data, 
                                                                        admin, refId, esign_response, payment=True)

                            logger.info(f"PU7-- REF ID : {reference['refId']}, ESTAMP DATA : {eStamp_data}, ESTAMP ID : {eStampId}",extra=logging_params)
                            logger.info(f"PU8-- REF ID : {reference['refId']}, REFERENCE : {reference}, FILE URL : {file_url}",extra=logging_params)

                            for estamp_ref in reference["estamp"]:
                                estamp_ref["status"] = "COMPLETED"
                                estamp_ref["payment_status"] = args['razorpay_payment_link_status']
                            reference["status"] = "IN_PROGRESS"
                            print("REFERENCE AFTER ESTAMP GENERATION FINAL : ", reference)

                            # Save unsigned docs to S3
                            src_path = destination_path
                            get_s3_storage = s3_storage_obj.save_document_s3(src_path, S3_CONFIG["unsigned_docs_folder"])
                            print("S3 STORAGE : ", get_s3_storage)
                            print("S3 STORAGE DOC LINK : ", get_s3_storage["doc_link"])
                            print("FILE PATH : ", get_s3_storage["file_path"])

                            # Update collections
                            cached_details['status'] = "COMPLETED"
                            cached_data_updated = Mongo.update_one(Mongo, cached_details, "payment_cache")

                            reference_updated = Mongo.update_one(Mongo, reference, "reference")

                            # Remove files from local storage
                            os.remove(destination_path)
                            os.remove(upload_destination)

                            url = payment_redirect_url['return_url'] + "?razorpay_payment_id=" + request.args["razorpay_payment_id"] + "&razorpay_payment_link_id=" + request.args["razorpay_payment_link_id"] + "&razorpay_payment_link_reference_id=" + request.args["razorpay_payment_link_reference_id"] + "&razorpay_payment_link_status=" + request.args["razorpay_payment_link_status"] + "&razorpay_signature=" + request.args["razorpay_signature"]
                            logger.info(f"PU21-- Redirecting to URL: {url}",extra=logging_params)
                            print(url)
                            return redirect(url)

                    else:
                        logger.info(f"PU2.1-- payment is already proccess for payment Id: {args['razorpay_payment_link_id']}",extra=logging_params)
                        raise CustomError(8061)
        except:
            try:
                os.remove(destination_path)
                os.remove(upload_destination)
            except:
                pass

# Cancel = api.model("Cancel", {
#    'remark': fields.String('534278')
# })


@api.route('/estamp/cancel/<orgId>/<refId>')
class eStampPayment(Resource):
    # @api.expect(Cancel)
    def get(self, orgId, refId):
        reference = Mongo.find_one_internal(
            Mongo, {"orgId": orgId, "refId": int(refId)}, "reference")
        reference['status'] = "CANCELLED"
        

        current_time = datetime.now()
        for esign in reference['eSign']:
            if esign['status'] == "in-progress":
                esign['status'] = "cancelled"
                eSign['updated_timestamp'] = current_time


        ref = Mongo.update_one(Mongo, reference, "reference")
        audit_trail = {}
        audit_trail['refId'] = int(refId)
        audit_trail['orgId'] = orgId
        audit_trail['step'] = "Document"
        #audit_trail['remark'] = api.payload['remark']
        audit_trail['status'] = "CANCELLED"
        audit_trail['datetime'] = datetime.now(tz_IST).strftime("%d-%B-%Y %X")
        ref = Mongo.insert_one(Mongo, audit_trail, "audit_trail")
        res = requests.get(eSign['url']+"/v1/cancel/"+orgId+"/"+refId)
        print(res.json())

        # Sending webhook notifications via Threads
        try:
            thread = Thread(target=initiateWebhookUpdate, args=[refId, orgId])
            thread.start()
            print(f"REFID : {refId} INITIATED REJECT WEBHOOK THREAD AT TIME : {datetime.now()}")
        except:
            error_msg = traceback.format_exc()
            print(f"REFID : {refId} EXCEPTION REJECT WEBHOOK : {error_msg}")

        return {
            "code": 200,
            "message": "Success",
            "status": {
                "created_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                "statusCode": 200,
                "statusMessage": "Request has been cancelled"
            }
        }


Reject = api.model("Reject", {
    'remark': fields.String('534278')
})
Failed = api.model("Failed", {
    'remark': fields.String('534278')
})
Retry = api.model("Retry", {
    'remark': fields.String('534278')
})

@api.route('/esign/reject/<orgId>/<refId>/<order>')
class Reject(Resource):
    @api.expect(Reject)
    def post(self, orgId, refId, order):
        logging_params={"endpoint":"reject_endpoint"}
        logger.info(f"REJECT 1 : REFID : {refId} INSIDE THE REJECT ENDPOINT", extra= logging_params)
        current_time = datetime.now().strftime('%d-%B-%Y %X')

        # HIT ESIGN FAILED ENDPOINT AND THEN UPDATE REFERENCE COLLECTION
        payload = {"remark": api.payload['remark']}
        headers = {
            'Content-Type': 'application/json'
        }

        res = requests.post(eSign['url']+"/v1/reject/"+orgId+"/" +
                            refId+"/"+order, data=json.dumps(payload), headers=headers)
        logger.info(f"REJECT 2 : REFID : {refId} RESPONSE : {res}", extra= logging_params)
        reference = Mongo.find_one_internal(
            Mongo, {"orgId": orgId, "refId": int(refId)}, "reference")
        logger.info(f"REJECT 3 : REFID : {refId} REFERENCE :  {reference}", extra= logging_params)
        if reference:
            reference['status'] = "REJECTED"
            for esign in reference['eSign']:
                if esign['order'] == int(order):
                    esign['status'] = "rejected"
                    esign['remark'] = api.payload['remark']
                    esign['updated_timestamp'] = str(current_time)
            ref = Mongo.update_one(Mongo, reference, "reference")
            esign = Mongo.find_one_internal(
                Mongo, {"orgId": orgId, "refId": int(refId), "order": int(order)}, "esign")
            logger.info(f"REJECT 4 : REFID : {refId} ESIGN :  {esign}", extra= logging_params)
            esign['status'] = "REJECTED"
            esign_updated = Mongo.update_one(Mongo, esign, "esign")
            audit_trail = {}
            audit_trail['refId'] = int(refId)
            audit_trail['orgId'] = orgId
            audit_trail['step'] = "eSign"
            audit_trail['status'] = "REJECTED"
            audit_trail['remark'] = api.payload['remark']
            audit_trail['order'] = esign['order']
            audit_trail['user'] = esign['name']
            audit_trail['docUrl'] = esign['uploadUrl']
            audit_trail['datetime'] = datetime.now(
                tz_IST).strftime("%d-%B-%Y %X")
            ref = Mongo.insert_one(Mongo, audit_trail, "audit_trail")
        
        # print(res.json())

        # Sending webhook notifications via Threads
        try:
            thread = Thread(target=initiateWebhookUpdate, args=[refId, orgId])
            thread.start()
            logger.info(f"REJECT 5 : REFID : {refId} INITIATED REJECT WEBHOOK THREAD AT TIME : {datetime.now()}", extra= logging_params)
        except:
            error_msg = traceback.format_exc()
            logger.info(f"REJECT 6 : REFID : {refId} EXCEPTION REJECT WEBHOOK : {error_msg}", extra= logging_params)

        if "return_url" in esign:
            return {
                "code": 200,
                "message": "Success",
                "status": {
                    "created_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                    "statusCode": 200,
                    "statusMessage": "Rejected"
                },
                "return_url" : esign["return_url"]
            }
        else:
            return {
                "code": 200,
                "message": "Success",
                "status": {
                    "created_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                    "statusCode": 200,
                    "statusMessage": "Rejected"
                }
            }

@api.route('/esign/failed/<orgId>/<refId>/<order>')
class Failed(Resource):
    @api.expect(Failed)
    def post(self, orgId, refId, order):
        remark = api.payload['remark']
        # UPDATE ESIGN db - REQUESTS
        request = Mongo.find_one_internal_esign(
            Mongo, {"orgId": orgId, "refId": int(refId), "order": int(order)}, "requests")
        
        current_time = datetime.now()
        print(f"REFID:{refId} - Request : {request}")
        if request:
            request['status'] = "failed"
            request['remark'] = remark
            request['updated_timestamp'] = current_time
            req = Mongo.update_one_esign(Mongo, request,"requests")
            print(f"REFID:{refId} - Failed Status updated in REQUESTS - REMARK : {api.payload['remark']}")


        # UPDATE PMS db - REFERENCE
        reference = Mongo.find_one_internal(
            Mongo, {"orgId": orgId, "refId": int(refId)}, "reference")
        print(reference, orgId, refId)
        if reference:
            reference['status'] = "FAILED"
            for esign in reference['eSign']:
                if esign['order'] == int(order):
                    esign['status'] = "failed"
                    eSign['remark'] = remark
                    eSign['updated_timestamp'] = current_time
            
            esign = Mongo.find_one_internal(
                Mongo, {"orgId": orgId, "refId": int(refId), "order": int(order)}, "esign")
            esign['status'] = "FAILED"
            esign_updated = Mongo.update_one(Mongo, esign, "esign")
            print(f"REFID:{refId} - Failed Status updated in ESIGN collection")
            audit_trail = {}
            audit_trail['refId'] = int(refId)
            audit_trail['orgId'] = orgId
            audit_trail['step'] = "eSign"
            audit_trail['status'] = "failed"
            audit_trail['remark'] = remark
            audit_trail['order'] = esign['order']
            audit_trail['user'] = esign['name']
            audit_trail['docUrl'] = esign['uploadUrl']
            audit_trail['datetime'] = datetime.now(
                tz_IST).strftime("%d-%B-%Y %X")
            ref = Mongo.insert_one(Mongo, audit_trail, "audit_trail")
            print(f"REFID:{refId} - Failed Status updated in AUDIT TRAIL ")
            ref = Mongo.update_one(Mongo, reference, "reference")
            print(f"REFID:{refId} - Failed Status updated in REFERENCE")
        
        return {
            "code": 200,
            "message": "Success",
            "status": {
                "created_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                "statusCode": 200,
                "statusMessage": "eSign Failed"
            }
        }

@api.route('/esign/retry/<orgId>/<refId>/<order>')
class Retry(Resource):
    @api.expect(Retry)
    def post(self, orgId, refId, order):
        # UPDATE ESIGN db - REQUESTS
        request = Mongo.find_one_internal_esign(
            Mongo, {"orgId": orgId, "refId": int(refId), "order": int(order)}, "requests")
        
        print(f"REFID:{refId} - Request : {request}")
        if request:
            request['status'] = "in-progress"
            if 'retry_count' in request:
                if request['retry_count'] >= 20:
                    return {
                        "code": 200,
                        "message": "Success",
                        "status": {
                            "created_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                            "statusCode": 200,
                            "statusMessage": "eSign Retry count exceeded",
                            "retryCount" : request['retry_count']
                        }
                    }
                else:
                    request['retry_count'] = request['retry_count'] + 1

            else:
                request['retry_count'] = 1
            
            remark = f"Retried failed document with Count : {request['retry_count']}"
            request['remark'] = remark
            req = Mongo.update_one_esign(Mongo, request,"requests")
            print(f"REFID:{refId} - Retry Status updated in REQUESTS - REMARK : {api.payload['remark']}")


        # UPDATE PMS db - REFERENCE
        reference = Mongo.find_one_internal(
            Mongo, {"orgId": orgId, "refId": int(refId)}, "reference")
        print(reference, orgId, refId)
        # if reference :
        if reference and (reference['status'] not in ['COMPLETED', 'REJECTED']):
            reference['status'] = "IN_PROGRESS"
            for esign in reference['eSign']:
                if esign['order'] == int(order):
                    esign['status'] = "in-progress"
                    eSign['remark'] = remark
            
            esign = Mongo.find_one_internal(
                Mongo, {"orgId": orgId, "refId": int(refId), "order": int(order)}, "esign")
            esign['status'] = "IN_PROGRESS"
            esign_updated = Mongo.update_one(Mongo, esign, "esign")
            print(f"REFID:{refId} - Retry Status updated in ESIGN collection")
            audit_trail = {}
            audit_trail['refId'] = int(refId)
            audit_trail['orgId'] = orgId
            audit_trail['step'] = "eSign"
            audit_trail['status'] = "in-progress"
            audit_trail['remark'] = remark
            audit_trail['order'] = esign['order']
            audit_trail['user'] = esign['name']
            audit_trail['docUrl'] = esign['uploadUrl']
            audit_trail['datetime'] = datetime.now(
                tz_IST).strftime("%d-%B-%Y %X")
            ref = Mongo.insert_one(Mongo, audit_trail, "audit_trail")
            print(f"REFID:{refId} - Retry Status updated in AUDIT TRAIL ")
            ref = Mongo.update_one(Mongo, reference, "reference")
            print(f"REFID:{refId} - Retry Status updated in REFERENCE")
        
        return {
            "code": 200,
            "message": "Success",
            "status": {
                "created_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                "statusCode": 200,
                "statusMessage": "eSign Retry success",
                "retryCount" : request['retry_count']
            }
        }

@api.route('/procurement_listener')
class Listener(Resource):
    def post(self):
        update = api.payload['changedDocument']
        print(type(update))
        if update['status'] == "complete":
            if "stamp_url" in update:
                file = update['stamp_url']
                estamp = Mongo.find_one_internal(Mongo, {"order_uuid":update['procurement_id']}, "estamp")
                dictionary = Mongo.find_cond(Mongo, {"eStampId": str(estamp['_id'])}, "esign")
                r = requests.get(file, stream=True)
                chunk_size = 10
                print("__________________",file)
                #os.remove(base_path+"estamp_docs/offline_stamps/" + estamp['filename'])
                with open(base_path+"estamp_docs/offline_stamps/" + estamp['filename'], 'wb') as fd:
                    for chunk in r.iter_content(chunk_size):
                        fd.write(chunk)
                estamp_instance = Estamp()
                print(estamp['filename'])
                estamp_instance.append_estamp(
                    base_path+"estamp_docs/offline_stamps/" + estamp['filename'],
                    base_path+"estamp_docs/unsigned_docs/" + estamp['filename']
                    )
                if "custom_reference" in dictionary[0]:
                    if len(dictionary[0]["custom_reference"])>0:
                        estamp_instance.addCertificateNo(base_path+"estamp_docs/unsigned_docs/" + estamp['filename'],update['grn_number'],dictionary[0]["custom_reference"])
                    else:
                        estamp_instance.addCertificateNo(base_path+"estamp_docs/unsigned_docs/" + estamp['filename'],update['grn_number'])
                else:
                    estamp_instance.addCertificateNo(base_path+"estamp_docs/unsigned_docs/" + estamp['filename'],update['grn_number'])
                onboard = InvestorOnboarding()
                estampInstance = Estamp()
                audit_trails = Mongo.find_cond(Mongo, {
                    "orgId": dictionary[0]['orgId'],
                    "refId": dictionary[0]['refId'],
                    "order_uuid": estamp['order_uuid'],
                    "step": "eStamp",
                    "status": "COMPLETED"},
                    "audit_trail")
                if len(audit_trails) == 0:
                    audit_trail = {}
                    audit_trail['refId'] = dictionary[0]['refId']
                    audit_trail['orgId'] = dictionary[0]['orgId']
                    audit_trail['step'] = "eStamp"
                    audit_trail['status'] = "COMPLETED"
                    audit_trail['order_uuid'] = estamp['order_uuid']
                    audit_trail['docUrl'] = eSign["url"]+"/estamp_docs/unsigned_docs/" + estamp['filename']
                    audit_trail['datetime'] = datetime.now(tz_IST).strftime("%d-%B-%Y %X")
                    ref = Mongo.insert_one(Mongo, audit_trail, "audit_trail")
                referenceInstance = Mongo.find_one_internal(Mongo, {
                     "orgId":dictionary[0]['orgId'],
                    'refId': dictionary[0]['refId']},
                    "reference")
                if "estamp" in referenceInstance:
                    for refe in referenceInstance['estamp']:
                        if refe['order_uuid'] == estamp['order_uuid']:
                            refe['docUrl'] = base_path+"estamp_docs/unsigned_docs/" + estamp['filename']
                            refe['created_at'] = datetime.now(tz_IST)
                            refe['status'] = "COMPLETED"
                estamp['status'] = "COMPLETED"
                estamp_id = estamp['_id']
                estamp_update = Mongo.update_one(Mongo, estamp, "estamp")
                ref_update = Mongo.update_one(Mongo, referenceInstance, "reference")
                for ref in dictionary:
                    print("testtt")
                    res = estampInstance.updateEsign(eSign["url"]+"/estamp_docs/unsigned_docs/" + estamp['filename'], ref['requestId'])
                    print("res++++++++++++++++++++++++++++++++++++++++++++++++++++++++++",res)
                response = onboard.process(str(estamp_id))
                print("****************** PROCESS RESPONSE *******************",response)

Process = api.model(
    "Process", {"data": fields.String("534278"), "orgId": fields.String("534278")}
)
import codecs

@api.route("/template_export")
class Process(Resource):
    @api.expect(Process)
    def post(self):
        templateId = api.payload["templateId"]
        base64_string = api.payload["base64"]
        file_name = api.payload["filename"]
        base64_pdf_bytes = base64_string.encode('utf-8')
        with open(base_path+'estamp_docs/uploads/'+file_name, 'wb') as file_to_save:
            file_to_save.write(codecs.decode(base64_pdf_bytes, "base64"))
        return

logger_estamp_listener = logging.getLogger("estamp_listener")
@api.route("/estamp_listener")
class Listener(Resource):
    def post(self):
        # eStampStatuscheck(api.payload)
        eStampOnline(api.payload)
        return {
            "code": 200,
            "message": "success"
        }

logger_esign_listener = logging.getLogger("esign_listener")
@api.route("/esign_listener/<orgId>/<refId>")
class Listener(Resource):
    def post(self,orgId,refId):
        randomId = random.randrange(10000000, 10**8)
        logger_esign_listener.info(f"REFID : {refId}, RANDOMID : {randomId}, RECIVED PAYLOAD : {api.payload} AND TYPE : {type(api.payload)}")

        try:
            esign = json.loads(api.payload)
        except:
            if isinstance(api.payload, dict):
                logger_esign_listener.info(f"REFID : {refId}, RANDOMID : {randomId}, RECIEVED PAYLOAD IS DICT")
                esign = api.payload
        eSignStatuscheck(esign['changedDocument'], randomId)

upload_parsers = api.parser()
upload_parsers.add_argument('excel', location='files',
                           type=werkzeug.datastructures.FileStorage, required=False)

@api.route("/esign/bulk_upload")
@api.expect('upload_parsers')
class BulkUpload(Resource):
    def post(self):
        logging_params={"endpoint":"bulk_upload"}
        try:
            pms_workflows = pms["workflows"]
            invalid_rows = []

            args = upload_parsers.parse_args()
            data_args = parser.parse_args()
            logger.info(f"BU1: Uploaded data from post request::{args}", extra=logging_params)
            
            if (args['excel'] is None):
                    return {
                        "code": 400,
                        "message": "Failed",
                        "status": {
                            "created_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                            "statusMessage": "File is missing"
                        }
                    }
            
            if (data_args['orgId'] is None):
                    return {
                        "code": 400,
                        "message": "Failed",
                        "status": {
                            "created_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                            "statusMessage": "Org ID is missing"
                        }
                    }

            destination = base_path+"estamp_docs/excel/"

            if (args['excel'] is not None) and (data_args['orgId'] is not None):
                orgId = data_args['orgId']
                file_name = args['excel'].filename.replace(
                    " ", "_").replace("#", "")
                logger.info(f"BU2: Excel file location::{destination + file_name}", extra=logging_params)
                if os.path.exists(destination + file_name):
                    expand = 1
                    while True:
                        expand += 1
                        new_file_name = file_name.split(
                            ".xls")[0] + "(" + str(expand) + ").xls"
                        if os.path.exists(destination + new_file_name):
                            continue
                        else:
                            file_name = new_file_name
                            break
                file = '%s%s' % (destination, file_name)
                args['excel'].save(file)

                df = pd.read_excel(file)
                if df.shape[0] > 250:
                    return {
                        "code": 400,
                        "message": "Failed",
                        "status": {
                            "processed_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                            "statusMessage": "Excel Row limit exceeded!!"
                        }
                }

                unique_orgIds = df['orgId'].unique()

                logger.info(f"BU3: Unique Org IDs: {unique_orgIds}, Org ID : {orgId}", extra=logging_params)

                for org in unique_orgIds:
                    if org != orgId:
                        return {
                        "code": 400,
                        "message": "Failed",
                        "status": {
                            "processed_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                            "statusMessage": "Invalid Org ID is present in the excel!"
                            }
                        }

                # orgId = df['orgId'].iloc[0]
                workflow_type = df['workflow_type'].iloc[0]
                df['status'] = 'PENDING'
                df['timestamp'] = str(datetime.now())
                # batchId = str(ObjectId())
                batchId = Mongo.get_batch_sequence(Mongo, "bulk_sequences")
                # batchId = "B" + str(batchId)
                batchId = str(int(batchId))

                # Generate filename from the original filename, current datetime, random number and batchId
                s3_filename = file_name.split(".xls")[0] + "_" + \
                    str(datetime.now().strftime("%Y%m%d%H%M%S")) + "_" + \
                    str(random.randrange(10000000, 10**8)) + "_" + batchId + ".xlsx"
                old_file = '%s%s' % (destination, file_name)
                new_file = '%s%s' % (destination, s3_filename)
                os.rename(old_file, new_file)

                logger.info(f"BU3A: BATCH ID : {batchId}, OLD FILE : {old_file}, S3 Filename::{new_file}", extra=logging_params)
                # Upload file to S3
                s3 = s3Storage()
                s3_resp = s3.save_document_s3(new_file, doc_type=S3_CONFIG['bulk_upload_docs_folder'], 
                                    content_dispostion="attachment", 
                                    content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                logger.info(f"BU3B: S3 Response::{s3_resp}", extra=logging_params)

                presigned_link = s3.generate_presigned_urls(s3_resp['doc_link'])
                logger.info(f"BU3C: BATCH ID : {batchId}, S3 File Link::{presigned_link}", extra=logging_params)

                # Update the file path and doc link in the dataframe
                df['s3_doc_link'] = s3_resp['doc_link']
                df['s3_file_path'] = s3_resp['file_path']

                df['batchId'] = batchId
                df['rowNum'] = np.arange(1, len(df)+1)

                logger.info(f"BU3A: Dataframe::{df}", extra=logging_params)
                df = df.fillna("")

                logger.info(f"BU3C: BATCH ID : {batchId}, Dataframe::{df.head(5)}, Dataframe Shape : {df.shape}", extra=logging_params)

                # New Change
                inserted_ids = Mongo.insert_many(Mongo, df.to_dict('records'), "bulk_upload")

                logger.info(f"BU4: BATCH ID : {batchId}, Inserted IDs::{inserted_ids}", extra=logging_params)
                inserted_copy = []
                for inserted_id in inserted_ids:
                    inserted_id = str(inserted_id)
                    inserted_copy.append(inserted_id)
                
                inserted_ids = inserted_copy
                logger.info(f"BU5: BATCH ID : {batchId}, Changing IDs to string and updating, Inserted Ids Now::{inserted_ids}", extra=logging_params)
                
                # Removed Celery And Added Kafka....
                logger.info(f"BU5: BATCH ID : {batchId}, Starting Kafka integration....", extra=logging_params)

                kafka_publisher_url = eStamp['url_kafka'] + "/v1/kafka_publisher"
                headers = {
                        'apikey': eSign['apikey']
                        }
                # payload for kafka publisher route
                payload_publisher= {
                    "topic":"BulkUpload",
                    "data":{
                    "insertedData" : json.dumps(inserted_ids),
                    "batchId" : batchId,
                    "orgId" : orgId
                    }
                }
                # payload for kafka publisher function
                # payload = {
                #     "insertedData" : json.dumps(inserted_ids),
                #     "batchId" : batchId,
                #     "orgId" : orgId
                #     }

                logger.info(f"BATCH ID : {batchId}, PAYLOAD ::::{payload_publisher}", extra=logging_params)
                # kafka publisher route
                resp = requests.post(url = kafka_publisher_url, json = payload_publisher, headers=headers, timeout=20)
                
                # kafka publisher function
                # kafka_delivery = KafkaProducer().publisher("BulkUpload", payload)

                # os.remove(file)
                os.remove(new_file)
                # This is added to check kafka broker connection. In case of successful delivery response will be PASS
                # kafka_delivery_report = KafkaProducer().delivery()
                if resp.status_code == 200:
                    kafka_delivery_report = resp.json().get("message", "")
                else:
                    kafka_delivery_report = "Failed"

                logger.info(f"BU13, BATCH ID : {batchId}, Ending Kafka integration....{resp.json()}, {kafka_delivery_report}", extra=logging_params)
                if kafka_delivery_report != "Success":
                    return {
                    "code": 400,
                    "message": "Failed",
                    "status": {
                        "processed_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                        "statusMessage": "Excel Upload Failed, Broker Down"
                    }
                }
                return {
                    "code": 200,
                    "message": "Success",
                    "status": {
                        "batchId" : batchId,
                        "orgId" : orgId,
                        "invalidRows" : invalid_rows,
                        "created_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                        "statusMessage": "Excel Uploaded Successfully!!"
                    }
                }

        except:
            logger.info(f"BU14:  Bulk Upload Exception: {traceback.format_exc()}", extra= logging_params)
            try:
                os.remove(file)
            except:
                pass
            return {
                    "code": 400,
                    "message": "Failed",
                    "status": {
                        "processed_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                        "statusMessage": "Excel Upload Failed"
                    }
                }

@api.route("/batch_info/<orgId>/<batchId>")
class BulkUpload(Resource):
    def get(self, orgId, batchId):
        try:
            print("Batch ID recieved : ", batchId)
            print("Org ID recieved : ", orgId)
            
            batch_requests = get_batch_request(orgId, batchId)

            return {
                    "code": 200,
                    "message": "Success",
                    "status": {
                        "batchId" : batchId,
                        "batch_requests" : batch_requests,
                        "processed_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                        "statusCode": 200
                    }
                }

        except:
            print("Batch Request Error : ", traceback.format_exc())
            return {
                    "code": 400,
                    "message": "Failed",
                    "status": {
                        "error" : "Internal Server Error",
                        "processed_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                    }
                }

@api.route("/list_failed_batch_info/<orgId>/<batchId>")
class BulkUpload(Resource):
    def get(self, orgId, batchId):
        try:
            logger.info(f"Batch ID recieved : {batchId}")
            logger.info(f"Org ID recieved : {orgId}")
            
            batch_requests = get_failed_batch_request(orgId, batchId)

            return {
                    "code": 200,
                    "message": "Success",
                    "status": {
                        "batchId" : batchId,
                        "batch_requests" : batch_requests,
                        "processed_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                        "statusCode": 200
                    }
                }

        except Exception as e:
            logger.info(f"Batch Request Error : {e}")
            return {
                    "code": 400,
                    "message": "Failed",
                    "status": {
                        "error" : "Internal Server Error",
                        "processed_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                    }
                }

@api.route("/pms_candidate_info/<orgId>")
class BulkUpload(Resource):
    def get(self, orgId):
        try:
            print(f"Org ID recieved : {orgId}")
            
            candidate_info = get_all_pms_candidate_info(orgId)

            return {
                    "code": 200,
                    "message": "Success",
                    "status": {
                        "candidate_info" : candidate_info,
                        "processed_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                        "statusCode": 200
                    }
                }

        except:
            print("Batch Request Error : ", traceback.format_exc())
            return {
                    "code": 400,
                    "message": "Failed",
                    "status": {
                        "error" : "Internal Server Error",
                        "processed_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                    }
                }

@api.route("/delete/<orgId>/<refId>")
class DeleteData(Resource):
    def get(self, orgId, refId):
        print("Org ID recieved : ", orgId)
        print("Ref ID recieved : ", refId)

        ref_url = eStamp['local_url'] + "/v1/onboard/reference/" + f"{orgId}/{refId}"
        
        headers = {
                'apikey': eSign['apikey']
                }

        resp = requests.get(ref_url, headers=headers)

        ref_data = resp.json()
        filename = ref_data['filename']
        curTimestamp = datetime.now(tz_IST).strftime("%d-%B-%Y_%X.%f")[:-3]
        deleted_file = filename.split(".pdf")[0] + "_deleted_" + curTimestamp + ".pdf"

        print(f"REF RESPONSE : {ref_data}")
        print(f"FILENAME : {filename}")

        esign_delete = eSign['url'] + "/v1/delete_esign/" + f"{orgId}/{refId}/{filename}/{curTimestamp}"

        headers = {
                'apikey': eSign['apikey']
                }

        esign_resp = requests.get(esign_delete, headers = headers)

        if esign_resp.json()['code'] == 200:
            print("eSign Data Deletion Successful")

        try:
            print("Removing eStamp files if present")
            unsigned_doc = os.path.join(base_path, "estamp_docs/unsigned_docs", filename)
            unsigned_deleted = os.path.join(base_path, "estamp_docs/unsigned_docs", deleted_file)
            os.rename(unsigned_doc, unsigned_deleted)
            print(f"Deleted Unsigned Doc for filename : {filename}")

            signed_doc = os.path.join(base_path, "estamp_docs/signed_docs", filename)
            signed_deleted = os.path.join(base_path, "estamp_docs/signed_docs", deleted_file)
            os.rename(signed_doc, signed_deleted)
            print(f"Deleted Signed Doc for filename : {filename}")
        except:
            pass

        ref_coll = Mongo.find_one_internal(Mongo, {"orgId": orgId, "refId": int(refId)},"reference")
        print("Before Update : ", ref_coll)
        ref_coll['fileStatus'] = "DELETED"
        ref_coll['deletedTime'] = curTimestamp
        print("After Update : ", ref_coll)

        update = Mongo.update_one(Mongo,ref_coll,"reference")
        print("Update : ", update)
        return {
                "code": 200,
                "message": "Success",
                "status": {
                    "orgId" : orgId,
                    "refId" : refId,
                    "deleteStatus" : "Done",
                    "processed_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                    "statusCode": 200,
                }
            }

@api.route("/batch_ids/<orgId>")
class BulkUpload(Resource):
    def get(self, orgId):
        try:
            print("Org ID recieved : ", orgId)
            
            batchIds = get_all_batch_ids(orgId)
            total_batchIds = len(batchIds)

            print("Total batch Ids : ", total_batchIds)

            return {
                    "code": 200,
                    "message": "Success",
                    "status": {
                        "orgId" : orgId,
                        "batchIds" : batchIds,
                        "total_batchIds" : total_batchIds,
                        "processed_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X")
                    }
                }
        except:
            print("Get all Batch IDs Error : ", traceback.format_exc())

            return {
                    "code": 400,
                    "message": "Failed",
                    "status": {
                        "error" : "Internal Server Error",
                        "processed_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                    }
                }

@api.route('/estamp_only_request')
@api.expect(upload_parser)
class InitiateEstamp(Resource):
    @api.expect(parser)
    def post(self):
        logging_params={"endpoint":"estamp_only_request"}
        try:
            logger.info(f"ESTAMP ONLY 1: INSIDE THE ESTAMP ONLY REQUEST", extra= logging_params)
            start = time.time()
            args = upload_parser.parse_args()
            args1 = parser.parse_args()

            rem_list = ["data", "checkOrder", "reminder","reminder_duration", 
                "reminder_expiry", "return_url","signature_expiry",
                "otpRequired","templateId", "face_capture", "location_capture",
                "exact_match", "batchId", "appendTemplate", "draftId",
                "rectangle", "pageNo", "reason", "location", "selfie"
                ]
            [args1.pop(key) for key in rem_list]

            if (args['file'] is None):
                return {
                    "code": 200,
                    "message": "Failed",
                    "status": {
                        "created_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                        "statusCode": 8001,
                        "statusMessage": "File key is missing"
                    }
                }
            
            dictionary = args1

            admin = Mongo.find_one_internal(
                Mongo, {'_id': ObjectId(dictionary['orgId'])}, "organisations")
            
            if (admin["procurement_mode"] != "ONLINE") or (admin["estamp_only"] != "true"):
                return {
                    "code": 200,
                    "message": "Failed",
                    "status": {
                        "created_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                        "statusCode": 8001,
                        "statusMessage": "ONLINE only eStamp Procurement Mode is disabled"
                    }
                }
    
            destination = base_path+"estamp_docs/uploads/"
            

            file_name = saveFile(args, destination)
            logger.info(f"ESTAMP ONLY 2: FILENAME : {file_name}, DESTINATION: {destination}", extra= logging_params)

            dictionary['filename'] = file_name
            dictionary['uploadLocation'] = destination+file_name
            dictionary["multiEstamp"] = False
            dictionary["totalEstamps"] = 1

            if re.search(",", str(dictionary["stampDutyValue"])):
                dictionary["stampDutyValue"] = dictionary["stampDutyValue"].split(",")
                dictionary["multiEstamp"] = True
                dictionary["stampDuties"] = dictionary["stampDutyValue"]
                dictionary["totalEstamps"] = len(dictionary["stampDutyValue"])
                dictionary["stampDutyValue"] = dictionary["stampDutyValue"][0]
                

            logger.info(f"ESTAMP ONLY 3: ESTAMP WRAPPER DICTIONARY : {dictionary}", extra= logging_params)

            data = open(dictionary["uploadLocation"], "rb").read()
            dictionary["base64"] = base64.b64encode(data).decode("ascii")
            logger.info(f"ESTAMP ONLY 4: FILENAME : {file_name}, GOT BASE64", extra= logging_params)
            request_validator = RequestValidator()
            request_validator.estamp_validation(dictionary, admin)
            estamp = Estamp()
            eStamp_data, order_uuid = estamp.initiateOnlineWrapper(admin, dictionary)

            if (eStamp_data is not None) and (order_uuid is not None):
                refId = Mongo.get_estamp_sequence(Mongo, "sequences")
                # eStamp_data["orders"] = [{
                #     "order_no" : 1,
                #     "order_uuid" : order_uuid,
                #     "status" : "in-progress",
                #     "initiated_at" : datetime.now(tz_IST)
                #     }]
                refId = int(refId)
                eStamp_data["orders"] = [{
                    "order_no" : 1,
                    "order_uuid" : order_uuid,
                    "status" : "in-progress",
                    "initiated_at" : datetime.now(tz_IST)
                    }]
                eStamp_data["eStampRefId"] = refId
                eStamp_data["status"] = "IN_PROGRESS"

                eStampId = Mongo.insert_one(Mongo, eStamp_data, "estamp_wrapper")
                logger.info(f"ESTAMP ONLY 5: FILENAME : {file_name}, ORDER UUID : {order_uuid}", extra= logging_params)

                response = {
                    "code": 200,
                    "message": "Success",
                    "status": {
                        "eStampRefId" : int(refId),
                        "orderStatus" : eStamp_data["status"],
                        "created_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                        "statusCode": 2001,
                        "statusMessage": "Initiated Online ESTAMP"
                    }
                }
                logger.info(f"ESTAMP ONLY 6: TIME FOR COMPLETE API WITH ORDER UUID-{order_uuid} >> {time.time()-start}", extra= logging_params)
                return response
            
            else:
                logger.info(f"ESTAMP ONLY 7: INITIATE ESTAMP WRAPPER UNABLE TO ORDER ESTAMP", extra= logging_params)
                return {
                "code": 500,
                "message": "Failed",
                "status": {
                    "processed_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                    "statusCode": 5002,
                    "statusMessage": "Internal Server Error"
                }
            }

        except CustomError as custom_err:
            return {
                "code": 200,
                "message": "Failed",
                "status": {
                    "created_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                    "statusCode": custom_err.error_code,
                    "statusMessage": custom_err.msg
                }
            }
        except Exception:
            logger.info(f"ESTAMP ONLY 8: INITIATE ESTAMP WRAPPER TRACEBACK-{traceback.print_exc()}", extra= logging_params)
            return {
                "code": 500,
                "message": "Failed",
                "status": {
                    "processed_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                    "statusCode": 5001,
                    "statusMessage": "Internal Server Error"
                }
            }

@api.route('/estampWithoutSignature/setWebhook/<orgId>')
class Webhook(Resource):
    def post(self, orgId):
        subscribeUrl = request.form.get('subscribeUrl')
        headers = request.form.get('headers')
        try:
            headers = ast.literal_eval(headers)
        except:
            pass
        
        webhook = Mongo.find_one_internal(Mongo, {"orgId": orgId}, "estamp_webhooks")
        print(f"ORG ID : {orgId}, SUBSCRIBE URL: {subscribeUrl}, HEADERS : {headers}", flush=True)
        
        if webhook is None:
            # save this url in database for a given orgId, refId
            insert_data = {
                "orgId": orgId,
                "subscribeUrl": subscribeUrl,
                "headers" : headers
            }
            webhookId = Mongo.insert_one(Mongo, insert_data, 'estamp_webhooks')
            if webhookId is not None:
                return ({
                    "code": 200,
                    "message": "Webhook set successfully"
                })
            else:
                return ({
                    "code": 500,
                    "message": "Webhook request failed"
                })
        else:
            webhook['subscribeUrl'] = subscribeUrl
            webhook["headers"] = headers
            webhookId = Mongo.update_one(Mongo, webhook, 'estamp_webhooks')
            if webhookId is not None:
                return ({
                    "code": 200,
                    "message": "Webhook set successfully"
                })
            else:
                return ({
                    "code": 500,
                    "message": "Webhook request failed"
                })

@api.route("/test_webhook")
class Listener(Resource):
    def post(self):
        current_time = datetime.now()
        print(f"TIME : {current_time}, PAYLOAD : {api.payload}",flush=True)
        # current_time = datetime.now()
        # print(f"PAYLOAD : {api.payload}")
        return {
            "code": 200,
            "message": "success"
        }

@api.route("/execute_failed_batch")
class batchId(Resource):
    def post(self):
        logging_params={"endpoint":"execute_failed_batch"}
        try:
            batchId=request.headers["batchId"]
            logger.info(f"EFB1, BATCH ID : {batchId}", extra= logging_params)
            
            bulk_upload_rec= Mongo.find_cond(Mongo, {"batchId": batchId,"status":{"$in": ["FAILED","PENDING"]}}, "bulk_upload")
            logger.info(f"EFB2, BATCH ID : {batchId}, FAILED BATCH DATA TO EXECUTE : {bulk_upload_rec}", extra= logging_params)
            
            res=BulkUpload_celery.delay(bulk_upload_rec)
            logger.info(f"EFB3, BATCH ID : {batchId}, FAILED BATCH DATA SENT TO EXECUTE", extra= logging_params)
            
            return {
                "code": 200,
                "status": "Upload successfull"
            }
        except Exception as e:
            logger.info(f"EFB0, EXCEPTION : {e}", extra= logging_params)
            return {
            "code": 500,
            "status": "Upload Failed"
        }

@api.route("/automated_esign/set_config")
class AutomatedEignConfig(Resource):
    def post(self):
        logging_params = {"endpoint":"automated_esign"}
        try:
            orgId = request.form.get('orgId')
            server_esign_url = request.form.get('server_esign_url')
            headers = request.form.get('headers')
            allowed_signature_types = request.form.get('allowed_signature_types')
            try:
                headers = ast.literal_eval(headers)
                allowed_signature_types = ast.literal_eval(allowed_signature_types)
            except:
                pass
            
            automated_esign = Mongo.find_one_internal(Mongo, {"orgId": orgId}, "automated_esign_config")
            logger.info(f"ASC1, ORG ID : {orgId}, SERVER ESIGN URL: {server_esign_url}, HEADERS : {headers}, ALLOWED SIGNATURE TYPES : {allowed_signature_types}", extra=logging_params)

            logger.info(f"ASC2, ORG ID : {orgId}, AUTOMATED ESIGN FROM DB : {automated_esign}", extra=logging_params)
            if automated_esign is None:
                # save this url in database for a given orgId, refId
                insert_data = {
                    "orgId": orgId,
                    "server_esign_url": server_esign_url,
                    "headers" : headers,
                    "allowed_signature_types" : allowed_signature_types
                }
                automated_esign_id = Mongo.insert_one(Mongo, insert_data, 'automated_esign_config')
                logger.info(f"ASC3, ORG ID : {orgId}, ADDED AUTOMATED ESIGN ID : {automated_esign_id}", extra=logging_params)
                
                if automated_esign_id is not None:
                    return {
                            "code": 200,
                            "message": "Success",
                            "status": {
                                "created_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                                "statusCode": 2050,
                                "statusMessage": "Configuration set successfully"
                        }
                    }
                else:
                    raise CustomError(8023)
            else:
                automated_esign["headers"] = headers
                automated_esign["server_esign_url"] = server_esign_url
                automated_esign["allowed_signature_types"] = allowed_signature_types

                automated_esign_id = Mongo.update_one(Mongo, automated_esign, 'automated_esign_config')
                logger.info(f"ASC4, ORG ID : {orgId}, UPDATED AUTOMATED ESIGN ID : {automated_esign_id}", extra=logging_params)
                
                if automated_esign_id is not None:
                    return {
                            "code": 200,
                            "message": "Success",
                            "status": {
                                "created_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                                "statusCode": 2050,
                                "statusMessage": "Configuration set successfully"
                        }
                    }
                else:
                    raise CustomError(8024)
        
        except CustomError as custom_error:
            error = traceback.format_exc()
            logger.info(f"ASC5, ORG ID : {orgId}, CUSTOM EXCEPTION TRACEBACK : {error}", extra=logging_params)
            return {
                        "code": 200,
                        "message": "Failed",
                        "status": {
                            "created_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                            "statusCode": custom_error.error_code,
                            "statusMessage": custom_error.msg
                    }
                }
        except Exception as e:
            error = traceback.format_exc()
            logger.info(f"ASC6, REQUEST EXCEPTION TRACEBACK : {error}", extra=logging_params)
            return {
                        "code": 500,
                        "message": "Failed",
                        "status": {
                            "created_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                            "statusCode": 5001,
                            "statusMessage": "Internal Server Error"
                    }
                }

@api.route('/manual_webhook_push_data')
class Webhook(Resource):
    def post(self):
        if 'refId' not in api.payload:
            return ({
                "code": 200,
                "message": "refId is missing"
            })
        elif 'orgId' not in api.payload:
            return ({
                "code": 200,
                "message": "orgId is missing"
            })
        elif 'changedDocument' not in api.payload:
            return ({
                "code": 200,
                "message": "changedDocument is missing"
            })
        
        refId = api.payload['refId']
        orgId = api.payload['orgId']
        referenceInstance = api.payload['changedDocument']

        status = initiateWebhookUpdate(refId, orgId, referenceInstance)

        if status == True:
            return ({
                "code": 200,
                "message": "Manual webhook trigger successfull"
            })
        else:
            return ({
                    "code": 500,
                    "message": "Manual webhook trigger failed"
                })


#route to be check whether a organisation needs to allow signing from india.
@api.route("/location_required/<orgId>")
class location_required(Resource):
    def get(self,orgId):
        logging_params={"endpoint":"LOCATION_REQUIRED"}
        logger.info(f"LR1, ORGID : {orgId}", extra=logging_params)
        org_conf=Mongo.find_one_internal(Mongo, {"_id": ObjectId(orgId)}, "organisations")
        
        logger.info(f"LR2, ORG DATA : {org_conf}", extra=logging_params)
        if "allow_signing_from_india" in org_conf and org_conf["allow_signing_from_india"]== "true":
            logger.info(f"LR3, ALLOW SIGNING FROM INDIA REQUIRED : {org_conf}", extra=logging_params)
            return ({
                            "code": 200,
                            "message": "Success",
                            "location_required":"true",

                        })
        else:
            logger.info(f"LR4, ALLOW SIGNING FROM IN INDIA NOT REQUIRED : {org_conf}", extra=logging_params)
            return ({
                            "code": 200,
                            "message": "Success",
                            "location_required":"false",

                        })
        
#route to be check whether a organisation needs to allow signing from india.
@api.route("/location_required/<orgId>")
class location_required(Resource):
    def get(self,orgId):
        logging_params={"endpoint":"LOCATION_REQUIRED"}
        logger.info(f"LR1, ORGID : {orgId}", extra=logging_params)
        org_conf=Mongo.find_one_internal(Mongo, {"_id": ObjectId(orgId)}, "organisations")
        
        logger.info(f"LR2, ORG DATA : {org_conf}", extra=logging_params)
        if "allow_signing_from_india" in org_conf and org_conf["allow_signing_from_india"]== "true":
            logger.info(f"LR3, ALLOW SIGNING FROM INDIA REQUIRED : {org_conf}", extra=logging_params)
            return ({
                            "code": 200,
                            "message": "Success",
                            "location_required":"true",

                        })
        else:
            logger.info(f"LR4, ALLOW SIGNING FROM IN INDIA NOT REQUIRED : {org_conf}", extra=logging_params)
            return ({
                            "code": 200,
                            "message": "Success",
                            "location_required":"false",

                        })
        
#route to be check whether a given pair of latitude and longitude is outside of India.
@api.route("/get_location")
class get_location(Resource):
    def post(self):

        logging_params={"endpoint":"GET_LOCATION"}
        latitude = api.payload['latitude']
        longitude=api.payload['longitude']
        # orgid=api.payload['orgId']
        # logger.info(f"G0, ORGID : {orgid}", extra=logging_params)
        logger.info(f"G1, LATITUDE {latitude} AND LONGITUDE{longitude}", extra=logging_params)
        
        try:
            
            latitude=float(latitude)
            longitude=float(longitude)
            outside_india=is_outside_india(latitude,longitude)
            logger.info(f"G2, LATITUDE AND LONGITUDE ARE NOT EMPTY", extra=logging_params)
        except:
            allow_signing="false"
            logger.info(f"G3, LATITUDE AND LONGITUDE ARE  EMPTY", extra=logging_params)
            return ({
                        "code": 200,
                        "message": "Success",
                        "allow_signing_from_india":allow_signing,
                        "remark":"Dear User This signing process requires the user to give access to location, Kindly please provide the access to the location .",

                    })
        
        
            
        if outside_india == True:
            logger.info(f"G5, LOCATION IS OUTSIDE INDIA", extra=logging_params)
            allow_signing="false"

            return ({
                        "code": 200,
                        "message": "Success",
                        "allow_signing_from_india":allow_signing,
                        "remark":"Dear User This signing process requires the user to be within India.Since the location captured is outside of India. We cannot process this request.",

                    })
        else:
            logger.info(f"G6, LOCATION IS NOT OUTSIDE INDIA", extra=logging_params)
            allow_signing="true"

            return ({
                        "code": 200,
                        "message": "Success",
                        "allow_signing_from_india": allow_signing,
                    })
        
        


#To set the wallet balance for online estamps
@api.route("/estamp/set_wallet_threshold")
class Webhook(Resource):
    def post(self):
        Threshold = request.form.get("estamp_wallet_threshold")
        orgId = request.form.get("orgId")
        estamp_wallet = Mongo.find_one_internal(Mongo, {"orgId": orgId}, "estamp_wallet")
        if estamp_wallet is None:
            # save this url in database for a given orgId, refId
            insert_data = {"orgId": orgId, "wallet_threshold": Threshold}
            estamp_walletId = Mongo.insert_one(Mongo, insert_data, "estamp_wallet")
            if estamp_walletId is not None:
                return {"code": 200, "message": "estamp_wallet set successfully"}
            else:
                return {"code": 500, "message": "estamp_wallet request failed"}
        else:
            estamp_wallet["wallet_threshold"]=Threshold
            Mongo.update_one(Mongo, estamp_wallet, "estamp_wallet")
            return {"code": 200, "message": "estamp_wallet set successfully"}
        

 
#to get the wallet balance from dqfy api
@api.route("/estamp/WalletBalance/<orgId>")
class WalletBalance(Resource):
    def get(self,orgId):
        logging_params={"endpoint":"WalletBalance"}

        logger.info(f"GET_WB1,INSIDE WALLET BALANCE ORGID : {orgId}", extra=logging_params)
        try:
            admin = Mongo.find_one_internal(
                    Mongo, {"_id": ObjectId(orgId)}, "organisations")
            if "auth_type" in admin and admin["auth_type"]=="test":
                url=eStampWallet["url_uat"]
                payload={}
                headers = {
                    'Content-Type': 'application/json',
                    'Authorization': "Bearer DOQFY " + admin["access_key"] + ":" + admin["secret_key"]
                }
                response = requests.request("GET", url+"/v1/client/account/wallet",
                                        headers=headers, data = json.dumps(payload))
                data=response.json()
                logger.info(f"GET_WB2,RESPONSE FOR UAT : {data}", extra=logging_params)
            
            else:
                url=eStampWallet["url_prod"]
                payload={}
                headers = {
                    'Content-Type': 'application/json',
                    'Authorization': "Bearer DOQFY " + admin["access_key"] + ":" + admin["secret_key"]
                }
                response = requests.request("GET", url+"/v1/client/account/wallet",
                                        headers=headers, data = json.dumps(payload))
                data=response.json()
                logger.info(f"GET_WB3,RESPONSE FOR PROD : {data}", extra=logging_params)
            
        
            return {
                            "code": 200,
                            "message": "Success",
                            "response": {
                                "created_at": datetime.now().strftime("%d-%B-%Y %X"),
                                "statusCode": 200,
                                "data": data["data"]
                            }
                    }
        except:
            return {"code": "400", "message": "Failed"}
        
@api.route("/health_check")
class Listener(Resource):
    def get(self):
        return {
            "code": 200,
            "message": "Success"
        }


@api.route("/estamp_bulk_requests/initiate_bulk_upload")
class Estamp_bulk_requests(Resource):
    def post(self):
        try:
            logging_params={"endpoint":"estamp_bulk_requests"}

            # Failed Response
            failed_response = {
                "code": 400,
                "message": "Failed",
                "status": {
                    "message": "Internal Server Error",
                    "processed_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                }
            }
            
            #add exception
            try:
                # Get JSON from request
                data = request.get_json()
                # data = api.payload
            except:
                failed_response['status']['message'] = "Unable fetch the request data"
                failed_response['status']['processed_at'] = datetime.now(tz_IST).strftime("%d-%B-%Y %X")
                return failed_response

            logger.info(f"ESBR 1: REQUEST DATA :{data}", extra=logging_params)
            
            if "quantity" not in data:
                failed_response['status']['message'] = "quantity is missing"
                failed_response['status']['processed_at'] = datetime.now(tz_IST).strftime("%d-%B-%Y %X")
                return failed_response

            if "denomination" not in data:
                failed_response['status']['message'] = "denomination is missing"
                failed_response['status']['processed_at'] = datetime.now(tz_IST).strftime("%d-%B-%Y %X")
                return failed_response

            if "orgId" not in data:
                failed_response['status']['message'] = "orgId is missing"
                failed_response['status']['processed_at'] = datetime.now(tz_IST).strftime("%d-%B-%Y %X")
                return failed_response

            if "region" not in data:
                failed_response['status']['message'] = "region is missing"
                failed_response['status']['processed_at'] = datetime.now(tz_IST).strftime("%d-%B-%Y %X")
                return failed_response

            if "article_number" not in data:
                failed_response['status']['message'] = "article_number is missing"
                failed_response['status']['processed_at'] = datetime.now(tz_IST).strftime("%d-%B-%Y %X")
                return failed_response
            
            if "firstPartyName" not in data:
                failed_response['status']['message'] = "firstPartyName is missing"
                failed_response['status']['processed_at'] = datetime.now(tz_IST).strftime("%d-%B-%Y %X")
                return failed_response
            
            if "secondPartyName" not in data:
                failed_response['status']['message'] = "secondPartyName is missing"
                failed_response['status']['processed_at'] = datetime.now(tz_IST).strftime("%d-%B-%Y %X")
                return failed_response
            
            if "stampDutyPaidBy" not in data:
                failed_response['status']['message'] = "stampDutyPaidBy is missing"
                failed_response['status']['processed_at'] = datetime.now(tz_IST).strftime("%d-%B-%Y %X")
                return failed_response
            
            if "purpose" not in data:
                failed_response['status']['message'] = "purpose is missing"
                failed_response['status']['processed_at'] = datetime.now(tz_IST).strftime("%d-%B-%Y %X")
                return failed_response

            orgId = data['orgId']

            # GET ALL AVAILABLE ESTAMPS FOR ORG ID
            try:
                url =f"{eStampInventory['url']}/api/v1/get_estamps_in_group_by_secondparty?org_id={orgId}"
                headers = {
                    'Content-Type': 'application/json',
                    "apikey": eStampInventory['apikey']
                    }
                available_estamps = requests.get(url, headers=headers)
                
                # available_estamp = Estamp.get_estamps_available(Estamp, data["orgId"])
                region_data = available_estamps.json()["data"][data["region"]]
                fetched_count = region_data.get(str(float(data["denomination"])))[data["firstPartyName"]].get(data["secondPartyName"])
                print("Fetched_count:", fetched_count)
                if int(data["quantity"]) > fetched_count:
                    return {
                        "code": 200,
                        "message": "Estamps are not available"
                    }

            except:
                raise CustomError(8038)
            
            batchId, all_insert_ids, kafka_delivery_report = process_estamp_inventory_bulk_upload(data)

            logger.info(f"ESBR 8: DELIVERY REPORT :{kafka_delivery_report}", extra=logging_params)
            if kafka_delivery_report == "Locking Failed":
                logger.info(f"ESBR 9: Locking Estamp is failed :{kafka_delivery_report}", extra=logging_params)
                failed_response['status']['message'] = "LOCKING ESTAMP FAILED"
                failed_response['status']['processed_at'] = datetime.now(tz_IST).strftime("%d-%B-%Y %X")
                return failed_response
            elif kafka_delivery_report != "Success":
                logger.info(f"ESBR 9: KAFKA DELIVERY REPORT IS FAILED :{kafka_delivery_report}", extra=logging_params)
                failed_response['status']['message'] = "KAFKA DELIVERY INSERTION FAILED"
                failed_response['status']['processed_at'] = datetime.now(tz_IST).strftime("%d-%B-%Y %X")
                return failed_response
                        
            logger.info(f"ESBR 10: KAFKA DELIVERY REPORT IS SUCCESS :{kafka_delivery_report}", extra=logging_params)
            return {
                "code": 200,
                "message": "Success",
                "status": {
                    "batchId" : batchId,
                    "orgId" : orgId,
                    "insert_ids" : all_insert_ids,
                    "created_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                }
            }
        except Exception as e:
            error_msg = generate_error_message(e, sys.exc_info())
            logger.info(f"ESBR 0A: ORGID : {orgId}, ERROR : {error_msg}")
            failed_response['status']['processed_at'] = datetime.now(tz_IST).strftime("%d-%B-%Y %X")
            return failed_response


@api.route("/estamp_bulk_requests/get_all_batch_ids/<orgId>")
class Estamp_bulk_requests(Resource):
    def get(self, orgId):
        try:
            logging_params={"endpoint":"estamp_bulk_requests"}

            # Failed Response
            failed_response = {
                "code": 400,
                "message": "Failed",
                "status": {
                    "message": "Internal Server Error",
                    "processed_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                }
            }
            
            output_data = get_all_estamp_bulk_request_batch_ids(orgId)

            return {
                "code": 200,
                "data": output_data,
                "count": len(output_data),
                "message": "Success"
            }
        
        except Exception as e:
            error_msg = generate_error_message(e, sys.exc_info())
            logger.info(f"GET EBR EBR ALL BATCHIDS, ORGID : {orgId}, ERROR : {error_msg}")
            failed_response['status']['processed_at'] = datetime.now(tz_IST).strftime("%d-%B-%Y %X")
            return failed_response

@api.route("/estamp_bulk_requests/batch_info")
class Estamp_bulk_requests(Resource):
    def post(self):
        try:
            logging_params={"endpoint":"estamp_bulk_requests"}

            # Failed Response
            failed_response = {
                "code": 400,
                "message": "Failed",
                "status": {
                    "message": "Internal Server Error",
                    "processed_at": datetime.now(tz_IST).strftime("%d-%B-%Y %X"),
                }
            }
            
            #add exception
            try:
                data = request.get_json()
            except:
                failed_response['status']['message'] = "Unable fetch the request data"
                failed_response['status']['processed_at'] = datetime.now(tz_IST).strftime("%d-%B-%Y %X")
                return failed_response
            
            if "orgId" not in data:
                failed_response['status']['message'] = "orgId is missing"
                failed_response['status']['processed_at'] = datetime.now(tz_IST).strftime("%d-%B-%Y %X")
                return failed_response
            
            if "batchId" not in data:
                failed_response['status']['message'] = "batchId is missing"
                failed_response['status']['processed_at'] = datetime.now(tz_IST).strftime("%d-%B-%Y %X")
                return failed_response
            
            batchId = int(data['batchId'])
            orgId = data['orgId']
            csv_data_string = get_estamp_bulk_request_batch_info(orgId, batchId)
            filename = str(batchId) + "_" + str(datetime.now()) + "_" + str(random.randint(10000, 99999)) + ".csv"

            return {
                "code": 200,
                "file": csv_data_string,
                "filename": filename,
                "message": "Success"
            }
        
        except Exception as e:
            error_msg = generate_error_message(e, sys.exc_info())
            logger.info(f"GET EBR BINFO, ORGID : {data['orgId']}, BATCH ID :{data['batchId']}, ERROR : {error_msg}")
            failed_response['status']['message'] = "Internal Server Error"
            failed_response['status']['processed_at'] = datetime.now(tz_IST).strftime("%d-%B-%Y %X")
            return failed_response
