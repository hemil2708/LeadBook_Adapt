import pymongo
import random
import scrapy
from scrapy.http import HtmlResponse
from scrapy.cmdline import execute
import json
import re
import requests
import datetime
import os
import requests

class DataSpider(scrapy.Spider):
    name = 'Data'
    allowed_domains = ['WWW.example.com']
    start_urls = ['http://WWW.example.com/']
    host = 'localhost'
    port = '27017'
    db = 'Adapt'

    def start_requests(self):
        try:
            url = "https://www.adapt.io/directory/industry/telecommunications/A-1"
            headers = {
            'accept': ' text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'accept-encoding': ' gzip, deflate, br',
            'accept-language': ' en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6,gu;q=0.5',
            'user-agent': ' Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36',
            'Cookie': 'AWSALB=JT1KfOOMkyDqHMkYjaRjrJ5jn+r1RQUkjsBGuEbQ12D52jnI338NbV0lewCBwTb89PpQ8V6ik5PJbdfG/FzmizgaC5KDOrqVlytIUiaTCkXu7dw1zZbhDKMAFQr6; AWSALBCORS=JT1KfOOMkyDqHMkYjaRjrJ5jn+r1RQUkjsBGuEbQ12D52jnI338NbV0lewCBwTb89PpQ8V6ik5PJbdfG/FzmizgaC5KDOrqVlytIUiaTCkXu7dw1zZbhDKMAFQr6'
            }
            yield scrapy.FormRequest(url=url,headers=headers,callback=self.firstlevel,dont_filter=True)
        except Exception as e:
            print(e)

    def firstlevel(self,response):
        try:
            con = pymongo.MongoClient(f'mongodb://{self.host}:{self.port}/')
            mydb = con[self.db]
            conn1 = mydb['link']
            company_index = []
            link = response.xpath('//*[@class="DirectoryList_linkItemWrapper__3F2UE "]/a/@href').extract()
            name = response.xpath('//*[@class="DirectoryList_linkItemWrapper__3F2UE "]/a/text()').extract()
            for i,j in zip(link,name):
                h1 = {}
                h1['company name'] = j
                h1['source_url'] = i
                try:
                    conn1.create_index("company name", unique=True)
                    X = conn1.insert(dict(h1))
                    print("Data Inserted Succesfully..!!")
                except Exception as e:
                    print(e, "Please Check Your Coding")
                company_index.append(h1)
                res = requests.get(i)
                response = HtmlResponse(url="example.com", body=res.content)
            with open('company_index.json', 'a', encoding='utf-8') as outfile:
                json.dump(company_index, outfile)
            next = response.xpath('//*[contains(text(),"Next")]//@href').extract_first(default="")
            yield scrapy.FormRequest(url=next,callback=self.firstlevel,dont_filter=True)
            Atoz = response.xpath('//*[@class="DirectoryTopInfo_directoryTopInfoContainer__35RPf"]//div/a/@href').extract()
            for ii in Atoz:
                yield scrapy.FormRequest(url=ii,callback=self.firstlevel,dont_filter=True)


            File = open('company_index.json',"r")
            Data = File.read()
            body = json.loads(Data)
            company_profiles = []
            for i in body:
                url = i['source_url']
                res = requests.get(url)
                response = HtmlResponse(url="example.com",body=res.content)

                con = pymongo.MongoClient(f'mongodb://{self.host}:{self.port}/')
                mydb = con[self.db]
                conn = mydb['Data']
                if response.status == 200:

                    try:
                        Company_name = response.xpath('//*[@class="CompanyTopInfo_leftContentWrap__3gIch"]//text()').extract_first(default="").strip()
                    except Exception as e:
                        Company_name = ""
                        print(e)
                    #####---------------------------------page_save ----------------------------------------###########
                    html_path = f'D:\\adapt\\page_save\\html\\{datetime.datetime.now().strftime("%d%m%Y")}\\'
                    if not os.path.exists(html_path):
                        os.makedirs(html_path)

                    html_file_name = html_path + '\\' + str(Company_name) + "_" + '.html'
                    with open(html_file_name, 'w', encoding='utf-8') as f:
                        f.write(response.text)
                        f.close()

                    try:
                        Company_Location = ''.join(response.xpath('//*[@class="CompanyTopInfo_infoWrapper__12xGT"]//*[contains(text(),"Location")]/..//*[@itemprop="address"]/span//text()').extract()).strip()
                    except Exception as e:
                        Company_Location = ""
                        print(e)
                    try:
                        Company_Website = response.xpath('//*[@class="CompanyTopInfo_leftContentWrap__3gIch"]//*[@itemprop="url"]//text()').extract_first(default="").strip()
                    except Exception as e:
                        Company_Website = ""
                        print(e)

                    try:
                        Company_WebDomain = Company_Website.split("www.")[-1].strip()
                    except Exception as e:
                        Company_WebDomain = ""
                        print(e)
                    try:
                        Company_Industry = response.xpath('//*[contains(text(),"Industry")]/..//span[2]//text()').extract_first(default="").strip()
                    except Exception as e:
                        Company_Industry = ""
                        print(e)

                    try:
                        Company_employee_Size = response.xpath('//*[contains(text(),"Head Count")]/..//span[2]//text()').extract_first(default="").strip()
                    except Exception as e:
                        Company_employee_Size = ""
                        print(e)

                    try:
                        Company_revenue = response.xpath('//*[contains(text(),"Revenue")]/..//span[2]//text()').extract_first(default="").strip()
                    except Exception as e:
                        Company_revenue = ""
                        print(e)
                    try:
                        Contact_Details = []

                        try:
                            Contact_name = response.xpath('//*[@class="TopContacts_contactName__3N-_e"]//text()').extract()
                        except Exception as e:
                            Contact_name = ""
                        try:
                            Contact_Jobtitle = response.xpath('//*[@class="TopContacts_jobTitle__3M7A2"]//text()').extract()
                        except Exception as e:
                            Contact_Jobtitle = ""
                            print(e)
                        try:
                            Contact_email_domain = response.xpath('//*[@class="simpleButton mailPhoneBtn emailBtn"]//text()').extract()
                        except Exception as e:
                            Contact_email_domain = ""
                            print(e)
                        try:
                            contact_department = ''.join(response.xpath('//*[@class="ContactsByDepartment_departmentListItem__26ahT"]//*[@itemprop="name"]//text()').extract()).strip()
                        except Exception as e:
                            contact_department = ""
                            print(e)
                        for cn,cj,ce in zip(Contact_name,Contact_Jobtitle,Contact_email_domain):
                            d1 = {}
                            d1["Contact_name"] = cn
                            d1["Contact_Jobtitle"] = cj
                            d1["Contact_email_domain"] = ce.split("@")[-1].strip()
                            d1["contact_department"] = contact_department
                            Contact_Details.append(d1)
                    except Exception as e:
                        Contact_Details = []
                        print(e)

                    item = {}
                    item['Company_name'] = Company_name
                    item['Company_location'] = Company_Location
                    item['Company_website'] = Company_Website
                    item['Company_webdomain'] = Company_WebDomain
                    item['Company_industry'] = Company_Industry
                    item['Company_employee_size'] = Company_employee_Size
                    item['Company_revenue'] = Company_revenue
                    item['Contact_Details'] = Contact_Details
                    company_profiles.append(item)
                    try:
                        conn.create_index("Company_name", unique=True)
                        X = conn.insert(dict(item))
                        print("Data Inserted Succesfully..!!")
                    except Exception as e:
                        print(e, "Please Check Your Coding")
            with open('company_profiles.json', 'w', encoding='utf-8') as outfile:
                json.dump(company_profiles, outfile)

        except Exception as e:
            print(e)


# execute("scrapy crawl Data".split())