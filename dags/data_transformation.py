# data_transformations.py

import pandas as pd
import re

def transform_dataframe(dataframe):
    #functions
    #edo kanoume to energy consumption float ,apomakrinoume monades metris oste na xoume mono arithmous ,opou exoume blank to kanoume 99999
    #dimiourgoume sto telos mia kainourgia stili cleaned_converted_energy_consumption
    def energy_consumption(dataframe):
        def clean_and_convert_energy_consumption(energy_consumption_raw_str):
            if energy_consumption_raw_str == "blank":
                cleaned_energy_consumption = "99999"
            else:
                cleaned_energy_consumption = energy_consumption_raw_str.replace('\xa0', '').replace('.', '').replace(',', '.').replace("- Warmwasser enthalten","").replace('kWh/(m²·a)', '').strip()
            return float(cleaned_energy_consumption)
            # Apply the function to the price column
        dataframe['final_energy_consumption'] = dataframe['energy_consumption'].apply(clean_and_convert_energy_consumption)

        #if dataframe['property_area_final'].unique():
        return dataframe

    #Edo antikathisto stin arxiki stili energy_class tin h me tin g klasi gia na me volepsei argotera stin formula gia na vrisko tin energeiako klasi vasi tis energy consumption
    #Dimiourgo kainourgia stili tin energy_class_after_replacing_h_with_g
    def replacing_energy_class_h_into_g(dataframe):
        def replace_letters_h_g(energy_class_raw_str):
            if energy_class_raw_str == "H":
                replaced_energy_class =energy_class_raw_str.replace('H', 'G').strip()
            else:
                replaced_energy_class =energy_class_raw_str.strip()
            return replaced_energy_class
        
        dataframe['energy_class_after_replacing_h_with_g'] = dataframe['energy_class'].apply(replace_letters_h_g)

        #if dataframe['property_area_final'].unique():
        return dataframe


    #Vrika mia formula sto chatgpt pou antistoixei tis enegry_consumptions se energeiakes klasseis
    #Xrisimopoio tin kainourgia stili cleaned_converted_energy_consumption kai apo autin mesi tis sinartis energy_class_formula 
    #dimiourgo mia kainourgia stili tin energy_class_based_on_energy_consumption
    def energy_class_based_on_energy_consumption(dataframe):
        def energy_class_formula(energy_consumption_integer):
            if energy_consumption_integer == 99999:
                return "99999"
            elif energy_consumption_integer <= 30:
                return "A+"
            elif 30 < energy_consumption_integer <= 50:
                return "A"
            elif 50 < energy_consumption_integer <= 75:
                return "B"
            elif 75 < energy_consumption_integer <= 100:
                return "C"
            elif 100 < energy_consumption_integer <= 130:
                return "D"
            elif 130 < energy_consumption_integer <= 160:
                return "E"
            elif 160 < energy_consumption_integer <= 200:
                return "F"
            else:return "G"
        dataframe['energy_class_based_on_energy_consumption'] = dataframe['final_energy_consumption'].apply(energy_class_formula)
        
    #energy_class
        return dataframe


    #Edo dimiourgo en teli tin teliki energeiaki klasi , opou an den exei timi sto arxiko energy_class oairnei tin timi apo to energy_class_based_on_energy_consumption


    def assign_final_energy_class(dataframe):
        dataframe["final_energy_class"] = dataframe.apply(
            lambda row: row["energy_class"] if row["energy_class"] != "blank" else row["energy_class_based_on_energy_consumption"], 
            axis=1
        )
        return dataframe





    #Plot Area
    #Metratrepo tin polt area se float , apomakrino monades metris ,metrapeo blank se 99999 kai dimiourgo stili
    def plot_area_meters(dataframe):
        def clean_and_convert_plot_area(plot_area_raw_str):
            if plot_area_raw_str == "blank":
                cleaned_plot_area = "99999"
            else:
                cleaned_plot_area = plot_area_raw_str.replace('\xa0', '').replace('.', '').replace(',', '.').replace('m²', '').strip()
            return float(cleaned_plot_area)
            # Apply the function to the price column
        dataframe['final_plot_area'] = dataframe['plot_area'].apply(clean_and_convert_plot_area)

        #if dataframe['property_area_final'].unique():
        return dataframe


    #Property price square meters
    #Idia logiki me plot_area
    def square_meters(dataframe):
        def clean_and_convert_area(square_meters_raw_str):
            if square_meters_raw_str == "blank":
                cleaned_square_meters = "99999"
            else:
                cleaned_square_meters = square_meters_raw_str.replace('\xa0', '').replace('.', '').replace(',', '.').replace('m²', '').strip()
            return float(cleaned_square_meters)
            # Apply the function to the price column
        dataframe['final_property_area'] = dataframe['property_area'].apply(clean_and_convert_area)

        #if dataframe['property_area_final'].unique():
        return dataframe



    #Kapoies poleis erxontao apo to scraping me to postal code.O parakato kodikas eksipiretei sto na ta xorisei an exrontai mazi
    #Version for city_raw splitting
    def city_raw_transformation(dataframe):
        def keep_city(city_raw):
            try:
                return re.search(r'[^0-9]+[^0-9]*[^0-9]',city_raw).group(0).strip()
            except AttributeError:return city_raw
        #Dimiourgia stilis pou krata to city_after_splitting
        dataframe["city_after_splitting"] = dataframe['city'].apply(lambda x: keep_city(x))
        
    #an den vrei kati to kanei 99999 gia na elegxo an doulevei o elegxos kala
        def keep_postal_code(city_raw):
            try:
                return re.search(r'[0-9]+[0-9]*[0-9]',city_raw).group(0).strip()
            except AttributeError:return "99999"
        
        #Dimiourgia stilis pou krata to postal code after splitting, an den eixe kati fernei 99999  
        dataframe["postal_code_after_splitting"] = dataframe['city'].apply(lambda x: keep_postal_code(x))

        #DIAVASE TA PARAKATO SXOLIA

        #Ta tria parakato kommatia kodika den ksero an xreiazontai gia auto ta kano comment kai vlepoume
        #control for correct data splitting
        #def contains_digit(s):
        #    return bool(re.search(r'\d', s))
        
        #control for dataframe['city_after_splitting']
        #for city in dataframe['city_after_splitting'].unique():
        #    if contains_digit(city):
        #        return f"Problem : {city}"
        
        #control for dataframe['postal_code_after_splitting']
        #for postal_code in dataframe['postal_code_after_splitting'].unique():
        #    if not contains_digit(postal_code):
        #        return f"Problem : {postal_code}"    
        return dataframe



    #Dimiourgia telikis stilis postal code
    #last version tis sinartisis autis
    def assign_final_postal_code(dataframe):
        dataframe["final_postal_code"] = dataframe.apply(
            lambda row: row["postal_code"] if row["postal_code"] != "blank" else row["postal_code_after_splitting"], 
            axis=1
        )
        return dataframe

    # PALIA VERSION TIS SINARTISIS AUTIS
    def assign_final_postal_code_palia(dataframe):
        dataframe["final_postal_code"] = np.where(
            dataframe["postal_code"] != "blank", 
            dataframe["postal_code"], 
            dataframe["postal_code_after_splitting"]
        )
        return dataframe

    #Dimiourgia telikis stilis postal code

    def renaming_splittingcity_finalcity(dataframe):
        # Renaming a single column
        def final_city(city_str):
            try:
                cleaned_city = re.search(r'[/s]*[A-Za-zäüöß. ,-]+',city_str).group(0).strip()
            except AttributeError:cleaned_city = "99999"
            return cleaned_city
        dataframe['final_city'] = dataframe['city_after_splitting'].apply(final_city)
        dataframe['final_city'] =dataframe['final_city'].str.lower()
        return dataframe


    
    #transformation_of_price
    def property_price_raw(dataframe):
        def clean_and_convert_price(property_price_raw_str):
            
            if (property_price_raw_str == "auf Anfrage") or ("€" not in property_price_raw_str):
                # Auf Anfrage einai katopin sizitisis, vazo 99999 gia na kano tin metatropi se float sta ipoloipa
                cleaned_price_str = "99999 €"
            else:

                cleaned_price_str = property_price_raw_str.replace('\xa0', '').replace('.', '').replace(',', '.').strip()
            
            return cleaned_price_str
        
        def clean_currency_convert_into_float(property_price_final_raw_str):
            #cleaned_from_currency_str = property_price_final_raw_str.replace('€', '')
            return float(property_price_final_raw_str.replace('€', ''))

        # Apply the function to the price column
        dataframe['property_price_currency_in_place_string'] = dataframe['property_price'].apply(clean_and_convert_price)
        dataframe['final_property_price'] = dataframe['property_price_currency_in_place_string'].apply(clean_currency_convert_into_float)
        dataframe['final_property_price'] = dataframe['property_price_currency_in_place_string'].apply(clean_currency_convert_into_float)
        return dataframe



    #Epeksergasia ton domation
    def property_rooms(dataframe):
        def clean_property_rooms(property_rooms_raw_str):
            property_rooms_str = str(property_rooms_raw_str)
            try:
                cleaned_property_rooms_str = re.search(r'[0-9]+.*[0-9]*',property_rooms_str).group(0).strip().replace(",",".")
            except AttributeError:cleaned_property_rooms_str = "99999"
            return cleaned_property_rooms_str
        dataframe['final_property_rooms'] = dataframe['property_rooms'].apply(clean_property_rooms)
        return dataframe

    #Epeksergasia Orofon
    def property_floor(dataframe):
        def propery_floor_first_step(property_floor_raw_str_first):
            if 'Untergeschoss' in property_floor_raw_str_first:
                return 'Basement'
            elif 'Dachgeschoss' in property_floor_raw_str_first:
                return 'Attic'
            elif 'Erdgeschoss' in property_floor_raw_str_first:
                return 'Ground_Floor'
            elif 'Souterrain' in property_floor_raw_str_first:
                return 'Basement'
            else:
                return property_floor_raw_str_first
        
        dataframe["property_floor_first_step"] = dataframe['floor'].apply(propery_floor_first_step)
        def propery_floor_second_step(property_floor_raw_str_second):
            try:
                cleaned = re.search(r'[0-9]{1,3}',property_floor_raw_str_second).group(0).strip()
            except AttributeError:
                cleaned = property_floor_raw_str_second
            return cleaned
        dataframe["property_floor_second_step"] = dataframe['property_floor_first_step'].apply(propery_floor_second_step)

        def finalising_property_floor(to_be_finalised_str):
            if (to_be_finalised_str.isdigit() == False) and (to_be_finalised_str not in ["Basement","Attic","Ground_Floor"]):
                return "99999"
            else:return to_be_finalised_str
        dataframe["final_property_floor"] = dataframe['property_floor_second_step'].apply(finalising_property_floor)


        return dataframe

    #DIORTHOSI GIA COMMERCIAL H PRIVATE
    def commercial_or_private_provider_property(dataframe):
        def commercial_or_private(commercial_or_private_raw_str):
            if commercial_or_private_raw_str.strip() == "Gewerblicher Anbieter":
                return "commercial_provider"
            else:return "private_provider"
        dataframe["final_type_provider_property"] = dataframe['commercial_or_private_provider_property'].apply(commercial_or_private)
        return dataframe


    def category_of_home(dataframe):
        dataframe["category_of_home_first_step"] = dataframe.apply(
            lambda row: row["category_of_house"] if row["category_of_house"] != "blank" else row["property_type"], 
            axis=1
        )
        # Convert values in the 'City' column to lowercase
        dataframe['final_category_of_home_second_step'] = dataframe['category_of_home_first_step'].str.lower()

        def translating_category_of_home(category_home_str):
            if category_home_str == "apartment":
                return "apartment"
            elif category_home_str == "house":
                return "house"
            elif category_home_str == "penthouse":
                return "penthouse"
            elif category_home_str == "maisonette":
                return "maisonette"
            elif category_home_str == "terrassenwohnung":
                return "terrace_apartment "
            elif category_home_str == "loft":
                return "attic"
            elif category_home_str == "rohdachboden":
                return "raw_attic"
            elif category_home_str == "mehrfamilienhaus":
                return   "family_dwelling"     
            elif category_home_str == "doppelhaushälfte":
                return   "semi_detached_house"
            elif category_home_str == "einfamilienhaus":
                return   "single_family_house "     
            elif category_home_str == "bungalow":
                return   "bungalow"     
            elif category_home_str == "villa":
                return   "villa"     
            elif category_home_str == "reihenendhaus":
                return   "row_haus"     
            elif category_home_str == "reihenmittelhaus":
                return   "row_haus"  
            elif category_home_str == "stadthaus":
                return   "town_house"   
            else :
                return   "99999"
        dataframe["final_category_of_home"] = dataframe['final_category_of_home_second_step'].apply(translating_category_of_home)
                                                                                
        return dataframe

    def splitting_address_number(dataframe):
        # Convert values in the 'City' column to lowercase
        dataframe['final_address_number_first_step'] = dataframe['address'].str.lower()

        def number_separate(address_str):
            try:
                cleaned_number = re.search(r'[0-9]+[a-z]*[-]*[0-9]*',address_str).group(0).strip()
            except AttributeError:cleaned_number = "99999"
            return cleaned_number
        dataframe["final_address_number"] = dataframe['final_address_number_first_step'].apply(number_separate)

        def address_separate(address_raw_str):
            if address_raw_str == "straße nicht freigegeben":
                cleaned_address = "99999"
            else:
                try:
                    cleaned_address = re.search(r'[/s]*[a-zäüöß. -]+',address_raw_str).group(0).strip()
                except AttributeError:cleaned_address = "99999"
            return cleaned_address
        dataframe["final_address_name"] = dataframe['final_address_number_first_step'].apply(address_separate)
        return dataframe

    def construction_year(dataframe):
        def construction_year_control(construction_year_raw_str):
            construction_year_str = str(construction_year_raw_str)
            try:
                cleaned_construction_year = re.search(r'[0-9]{4}',construction_year_str).group(0).strip()
            except AttributeError:cleaned_construction_year = "99999"
            return cleaned_construction_year
        dataframe["final_construction_year"] =dataframe['construction_year'].apply(construction_year_control)
        return dataframe

    def offerer_name(dataframe):
        # Convert values in the 'City' column to lowercase
        dataframe['final_offerer_name'] = dataframe['offerer_name'].str.lower()
        return dataframe



        #Teliko Vima , Kratao mono stiles pou me endiaferoun
#Teliko Vima , Kratao mono stiles pou me endiaferoun
    def keep_only_columns_of_interest(dataframe):
        # List of columns to keep
        columns_to_keep = ['online_id', 'property_webpage', 'property_type', 'delivery_time','final_energy_consumption' , 'final_energy_class','final_plot_area','final_property_area','final_postal_code','final_property_price'
                           ,'final_property_rooms','final_property_floor','final_type_provider_property','final_category_of_home','final_construction_year','final_offerer_name','german_state','final_city']
        dataframe = dataframe[columns_to_keep]
        return dataframe
    
    def data_types_convert(dataframe):
        # Convert the 'deliverytime' column to datetime
        dataframe['delivery_time'] = pd.to_datetime(dataframe['delivery_time'])
        dataframe["final_construction_year"] = dataframe["final_construction_year"].astype(int)
        return dataframe
    
    def control_remove_duplicate_online_id_from_csv(dataframe):
        dataframe = dataframe.drop_duplicates(subset=['online_id'])
        return dataframe
    

    dataframe = energy_consumption(dataframe)
    dataframe =replacing_energy_class_h_into_g(dataframe)
    dataframe =energy_class_based_on_energy_consumption(dataframe)
    dataframe =assign_final_energy_class(dataframe)
    dataframe =plot_area_meters(dataframe)
    dataframe =square_meters(dataframe)
    dataframe =city_raw_transformation(dataframe)
    dataframe =assign_final_postal_code(dataframe)
    dataframe =renaming_splittingcity_finalcity(dataframe)
    dataframe =property_price_raw(dataframe)
    dataframe =property_rooms(dataframe)
    dataframe =property_floor(dataframe)
    dataframe =commercial_or_private_provider_property(dataframe)
    dataframe =category_of_home(dataframe)
    dataframe =splitting_address_number(dataframe)
    dataframe =construction_year(dataframe)
    dataframe =offerer_name(dataframe)
    dataframe = renaming_splittingcity_finalcity(dataframe)
    dataframe =keep_only_columns_of_interest(dataframe)
    dataframe = data_types_convert(dataframe)
    dataframe = control_remove_duplicate_online_id_from_csv(dataframe)
    return dataframe