import requests
from bs4 import BeautifulSoup
import pandas as pd
from time import sleep
from datetime import datetime
import json
import re

from web_scraper_helpers import (
    rooms_area_function, property_price_function, construction_year_function, 
    commercial_or_private_provider_property, location_details, floor, 
    title_property, ref_number, online_id, delivery_time, property_condition, 
    property_type, offerer_name, category_of_house, property_possible_move, 
    energy_consumption, energy_vadility, energy_passtype, energy_buildingtype, 
    energy_class, energy_source, energy_equipment, plot_area, german_state_function,connect_to_database
)


def scraper_function(webpages_param):
    all_dictionaries_of_apartments = []
    all_current_pages = []
    german_states_numbers_of_pages = {"bremen" : 20000}
    #{"hamburg": 20000,"bl-schleswig-holstein": 20000,"bremen" : 20000, "berlin" : 20000,"bl-hessen" : 20000,"bl-niedersachsen" : 20000
                                  #,"bl-baden-wuerttemberg" :20000,"bl-bayern" : 20000,"bl-mecklenburg-vorpommern" : 20000,"bl-nordrhein-westfalen" : 20000,"bl-rheinland-pfalz" :20000,"bl-saarland" :20000,"bl-sachsen" :20000,"bl-sachsen-anhalt" :20000,"bl-schleswig-holstein" :20000,"bl-thueringen" :20000}

    tipoi_akiniton = ["wohnungen","haeuser"]
    main_link_first_part = 'https://www.immowelt.de/suche/'
    main_link_second_part = '/kaufen?d=true&sd=DESC&sf=RELEVANCE&sp='
    pages = 20000
    for tipos in tipoi_akiniton:
        for german_state,number_each_state in german_states_numbers_of_pages.items():         
            german_state_part = german_state
            german_state_link_general = main_link_first_part + german_state_part + "/" + tipos +main_link_second_part
            for page in range(1, 20000):
                try:
                    german_state_link_specific = german_state_link_general+str(page)
                    response = requests.get(german_state_link_specific,timeout=15)
                    if response.status_code ==200:
                        try:
                            soup = BeautifulSoup(response.content, "html.parser")
                            all_properties_webpages_of_each_general_page = soup.find_all('a', {'class': 'mainSection-88b51 noProject-889ca'})
                            for property_webpage_raw in all_properties_webpages_of_each_general_page:
                                        property_webpage = property_webpage_raw.get('href')
                                        all_current_pages.append(property_webpage)
                                        if property_webpage in webpages_param:
                                            continue   
                                        single_property_response = requests.get(property_webpage,timeout=15)
                                        single_soup = BeautifulSoup(single_property_response.content, "html.parser")
                                        dict_single_apartment = {
                                            "property_webpage":  property_webpage,
                                            "online_id": online_id(single_soup) ,
                                            "ref_number": ref_number(single_soup) ,
                                            "property_type" :property_type(tipos) ,
                                            "title_property" : title_property(single_soup),
                                            "city": location_details(single_soup)[0] ,
                                            "address":  location_details(single_soup)[1],
                                            "postal_code":  location_details(single_soup)[2],
                                            "construction_year": construction_year_function(tipos,single_soup) ,
                                            "floor":  floor(single_soup),
                                            "commercial_or_private_provider_property": commercial_or_private_provider_property(single_soup),
                                            "property_rooms": rooms_area_function(single_soup)[0] ,
                                            "property_area":  rooms_area_function(single_soup)[1],
                                            "plot_area" : plot_area(single_soup),
                                            "property_price": property_price_function(single_soup) ,
                                            "energy_provider":energy_equipment(single_soup)[0] ,
                                            "form_of_heating": energy_equipment(single_soup)[1],
                                            "energy_class":energy_class(single_soup) ,
                                            "energy_buildingtype":energy_buildingtype(single_soup) ,
                                            "energy_passtype": energy_passtype(single_soup),
                                            "energy_vadility": energy_vadility(single_soup),
                                            "energy_consumption": energy_consumption(single_soup),
                                            "energy_source" :energy_source(single_soup) ,
                                            "offerer_name" : offerer_name(single_soup),
                                            "property_condition": property_condition(single_soup),
                                            "category_of_house" : category_of_house(single_soup),
                                            "property_possible_move" : property_possible_move(single_soup),
                                            "german_state" : german_state_function(german_state),
                                            "delivery_time" :delivery_time() 
                                        }
                                        all_dictionaries_of_apartments.append(dict_single_apartment)
                                        print(dict_single_apartment)
                                        sleep(2)
                        except: print("Page could not be loaded.")
                    else:break
                except IndexError:break
    return all_current_pages,webpages_param,all_dictionaries_of_apartments


#{"hamburg": 20000, "bl-schleswig-holstein": 20000,"bremen" : 20000, "berlin" : 20000,"bl-hessen" : 20000,"bl-niedersachsen" : 20000
#                                  ,"bl-baden-wuerttemberg" :20000,"bl-bayern" : 20000,"bl-mecklenburg-vorpommern" : 20000,"bl-nordrhein-westfalen" : 20000,"bl-rheinland-pfalz" :20000,"bl-saarland" :20000,"bl-sachsen" :20000,"bl-sachsen-anhalt" :20000,"bl-schleswig-holstein" :20000,"bl-thueringen" :20000}
