import json
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from time import sleep
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import WebDriverException

DRIVER_PATH = 'chromedriver.exe'
current_link = 1
turn_it = True
all_journals = list()

chrome_options = Options()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')

while turn_it:
    dico_journal = {}

    with open('urls_acm.json') as data_file:
        data = json.load(data_file)
    
    for v in data:
        driver = webdriver.Chrome(options=chrome_options)
        
        try:
            driver.get(v)
        except WebDriverException:
            print("Page down")
        
        sleep(4)

        try:
            element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'a.btn.blue.cookiePolicy-popup__close.pull-right'))
            )
            element.click()
        except Exception as e:
            print(f"Erreur lors de la fermeture de la fenêtre : {e}")

        authors = []
        locations = []

        try:
            fullpage = driver.find_element(By.XPATH, '//main')
        except:
            pass

        try:
            sectionOne = fullpage.find_element(
                By.XPATH, '//article/div[1]/div[2]/div[1]/div[1]')
        except:
            pass

        try:
            TypeAccessSection = sectionOne.find_element(By.XPATH, './div[1]')
        except:
            pass

        try:
            type = sectionOne.find_element(By.CLASS_NAME, 'issue-heading').text
        except:
            type = ""

        try:
            access = sectionOne.find_element(
                By.CLASS_NAME, 'article__access').text
        except:
            access = ""

        try:
            TitleSection = sectionOne.find_element(By.XPATH, './div[2]')
        except:
            pass

        try:
            title = TitleSection.find_element(By.XPATH, './h1').text
        except:
            title = ""

        try:
            AuthorSection = sectionOne.find_element(
                By.XPATH, './div[3]/div[1]/ul[1]')
        except:
            pass

        try:
            listauthors = AuthorSection.find_elements(
                By.CLASS_NAME, 'loa__item')
        except:
            pass

        try:
            for auth in listauthors:
                author = auth.find_element(
                    By.XPATH, './a[1]').get_attribute('title')
                authors.append(author)
        except:
            pass

        try:
            for auth in WebDriverWait(AuthorSection, 8).until(EC.visibility_of_all_elements_located((By.CLASS_NAME, "loa__item"))):
                listauthors = AuthorSection.find_elements(
                    By.CLASS_NAME, 'loa__item')
                WebDriverWait(auth, 8).until(
                    EC.element_to_be_clickable((By.XPATH, './a[1]'))).click()
                sleep(4)
                location = auth.find_element(By.XPATH, './div[1]/div[2]').text
                if (location != "Search about this author"):
                    location = location.replace(
                        '\nSearch about this author', '')
                    location = location.replace('\nView Profile', '')
                    locations.append(location)
                TitleSection.find_element(By.XPATH, './h1').click()
        except:
            pass

        try:
            ResourcesDOiSection = sectionOne.find_element(By.XPATH, './div[4]')
        except:
            pass

        try:
            publisher = ResourcesDOiSection.find_element(
                By.XPATH, './div[1]/a[1]/span[1]').text
        except:
            publisher = ""

        try:
            doi = str(ResourcesDOiSection.find_element(
                By.CLASS_NAME, 'issue-item__doi').get_attribute('href'))
        except:
            doi = ""

        try:
            DateSection = sectionOne.find_element(By.XPATH, './div[5]')
        except:
            pass

        try:
            date = DateSection.find_element(By.XPATH, './span[2]').text
        except:
            date = ""

        try:
            sectionTwoAbstract = fullpage.find_element(
                By.XPATH, '//article/div[2]/div[2]/div[2]/div[1]/div[1]/div[2]/div[1]')
        except:
            pass

        try:
            abstract = sectionTwoAbstract.find_element(By.XPATH, './p[1]').text
        except:
            abstract = ""

        try:
            sectionTreekey = fullpage.find_element(
                By.XPATH, '//article/div[2]/div[2]/div[2]/div[5]/ol[1]/li[1]')
        except:
            pass

        try:
            Keywords = sectionTreekey.find_element(By.XPATH, './h6').text
        except:
            Keywords = ""

        dico_journal = {"Title": title, "Abstract": abstract, "Type": type, "Access": access,
                        "Date": date, "DOI": doi, "Publisher": publisher, "Authors": authors, "Locations": locations}

        all_journals.append(dico_journal)
        print(current_link)
        current_link += 1

    driver.close()  # fermer la fenêtre
    driver.quit()   # fermer la session
    turn_it = False

with open("acm_data.json", "w") as write_file:
    json.dump(all_journals, write_file, indent=4)

