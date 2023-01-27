from datetime import date
import re

import scrapy
from scrapy.crawler import CrawlerProcess

from handlers.config import CITIES
from handlers.api_dataset_builder import GetApidata


class BookingSpider(scrapy.Spider):
    name = "boonkingfrance"
   
    def start_requests(self):
        for item in self.cities:
            yield scrapy.Request(f'https://www.booking.com/searchresults.html?ss={item["city_to_scrap"]}&nflt=ht_id=204&', meta = item, callback=self.pagination)
        
    def pagination(self, response):
        # Check number of page on the footer and replace the number of page to 0 if only 1 page to scrap
        try:
            number_of_page = int(response.xpath('.//ol[@class="a8b500abde"]/li[@class="f32a99c8d1"][last()]/button/text()').get())
        except Exception:
            number_of_page = 0
        
        # Small algo toLimit the scraping to 2 pages (offset max 50)
        max_offset = (number_of_page*25)
        if max_offset > 50:
            max_offset = 50    
        number_of_properties = re.findall(r'\d+',response.xpath('.//div[@class="efdb2b543b"]/h1/text()').get()) # To review because return list instead of int
        nearby_properties = response.xpath('.//div[@class="db29ecfbe2"]/span[@class="db29ecfbe2"]/text()').get()
        current_url = response.xpath('.//a[@class="fc63351294 a168c6f285 b7555bc87e"]/@href').get()
        
        
        # Get data from API (streetmap and weather) and construct the futur dataframe
        df_dict = {
            'id':response.request.meta['id'],
            'city_to_scrap':response.request.meta['city_to_scrap'],
            
            'city_lat':response.request.meta['lat'],
            'city_long':response.request.meta['lon'],
            'elevation':response.request.meta['elevation'],
            'days':response.request.meta['days'],
            'temp_max':response.request.meta['temp_max'],
            'temp_min':response.request.meta['temp_min'],
            'pre_sum':response.request.meta['pre_sum'],
            
            'number_of_properties':number_of_properties,
            'nearby_properties':nearby_properties,
            'current_url':current_url
        }
        
        # # Check for inconsistencies before scraping hotel's info
        if number_of_properties == 0 or nearby_properties:
            url_with_offset = f'{current_url}offset=0'
            yield scrapy.Request(url=url_with_offset, callback=self.get_hotel_list,meta=df_dict)
        else:
            for i in range(0,max_offset,25):
                url_with_offset = f'{current_url}offset={i}'
                yield scrapy.Request(url=url_with_offset, callback=self.get_hotel_list, meta=df_dict)
   
    def get_hotel_list(self, response):
        """
        Scrap properties (hotels) in the main page and 
        return list of dictionnary with the all the hotels details found in the page
        """
        for info in response.xpath('//div[@class="a826ba81c4 fe821aea6c fa2f36ad22 afd256fc79 d08f526e0d ed11e24d01 ef9845d4b3 da89aeb942"]'):
            hotel_name = info.xpath('.//div[@class="fcab3ed991 a23c043802"]/text()').get()
            starts = len(info.xpath('.//div[@class="fbb11b26f5"]/span'))
            hotel_city = response.xpath('//span[@class="f4bd0794db b4273d69aa"]/text()').get()
            hotel_comments = info.xpath('.//span[@class="a51f4b5adb"]/text()').get()
            hotel_description = info.xpath('.//div[@class="a1b3f50dcd f7c6687c3d ef8295f3e6"]/div[@class="d8eab2cf7f"]/text()').get()
            hotel_score = info.xpath('.//div[@class="b5cd09854e d10a6220b4"]/text()').get()
            hotel_url = info.xpath('.//a[@class="e13098a59f"]/@href').get()
            
            hotels_list = {
                'id':response.request.meta['id'],
                
                'city_to_scrap':response.request.meta['city_to_scrap'],
                'city_lat':response.request.meta['city_lat'],
                'city_long':response.request.meta['city_long'],
                'elevation':response.request.meta['elevation'],
                'days':response.request.meta['days'],
                'temp_max':response.request.meta['temp_max'],
                'temp_min':response.request.meta['temp_min'],
                'pre_sum':response.request.meta['pre_sum'],
                'number_of_properties':response.request.meta['number_of_properties'],
                'nearby_properties':response.request.meta['nearby_properties'],
                'current_url':response.request.meta['current_url'],
                'hotel_name': hotel_name,
                'hotel_starts': starts,
                'hotel_city': hotel_city,
                'hotel_comments': hotel_comments,
                'hotel_description': hotel_description,
                'hotel_score': hotel_score,
                'hotel_url': hotel_url,
                }
            
            yield response.follow(url=hotel_url, callback= self.get_hotel_details, meta = hotels_list)
            
    def get_hotel_details(self, response):
        """
        Scrap hotel information from the hotel page  and 
        return a list of hotel information e.g address, comments 
        """
        id=response.request.meta['id']

        city_to_scrap=response.request.meta['city_to_scrap']
        city_lat=response.request.meta['city_lat']
        city_long=response.request.meta['city_long']
        
        elevation=response.request.meta['elevation']
        days=response.request.meta['days']
        temp_max=response.request.meta['temp_max']
        temp_min=response.request.meta['temp_min']
        pre_sum=response.request.meta['pre_sum']
        
        number_of_properties=response.request.meta['number_of_properties']
        nearby_properties=response.request.meta['nearby_properties']
        current_url=response.request.meta['current_url']
        hotel_name=response.request.meta['hotel_name']
        hotel_starts=response.request.meta['hotel_starts']
        hotel_city=response.request.meta['hotel_city']
        hotel_comments=response.request.meta['hotel_comments']
        hotel_description=response.request.meta['hotel_description']
        hotel_score=response.request.meta['hotel_score']
        hotel_url=response.request.meta['hotel_url']
        
        hotel_address = (response.xpath('//span[@data-node_tt_id="location_score_tooltip"]/text()').get()).strip()
        hotel_geoposition = response.xpath('//a[@id="hotel_sidebar_static_map"]/@data-atlas-latlng').get()
        hotel_popular_facilities = response.xpath('.//div[@class="hp_desc_important_facilities clearfix hp_desc_important_facilities--bui "]//div[@class]/@data-name-en').getall()
        
        print(id,  city_lat, city_long, days[0], temp_max, hotel_name)
        
        yield{
            'id':id,
            'city_to_scrap':city_to_scrap,
            'city_lat':city_lat,
            'city_long':city_long,
            'elevation':elevation,
            'days':days,
            'temp_max':temp_max,
            'temp_min':temp_min,
            'pre_sum':pre_sum,
            'number_of_properties': number_of_properties,
            'nearby_properties': nearby_properties,
            'current_url': current_url,
            'hotel_city': hotel_city,
            'hotel_name': hotel_name,
            'hotel_address': hotel_address,
            'hotel_starts': hotel_starts,
            'hotel_description': hotel_description,
            'hotel_comments': hotel_comments,
            'hotel_geoposition': hotel_geoposition,
            'hotel_popular_facilities': hotel_popular_facilities,
            'hotel_score': hotel_score,
            'hotel_url': hotel_url,
        }


def main():
    # Intentiate the class to get the API data (weather and long lat)
    result_from_apis = GetApidata(CITIES).get_data()

    # Setup and run the scraper 
    today = date.today()
    filename = "enriched_dataset_booking" + ".json"
    process = CrawlerProcess(
        settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:105.0) Gecko/20100101 Firefox/105.0',
        # 'LOG_LEVEL': logging.INFO,
        'FEEDS': {'output/' + f'{filename} ': {"format": "json"}},
    })
    process.crawl(BookingSpider,cities=result_from_apis)
    process.start()


if __name__ == "__main__":
    main()
