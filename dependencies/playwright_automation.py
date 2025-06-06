import re
from dependencies import log
from settings import screenshot
import constants
import random
import json
import time

class Playwright_automation:
    def __init__(self):
        self.__logger = log.Log().get_logger(name=constants.log_file['log_name'])
        
        
    def check_home_page(self, page, list_mhtml_traffic):
        self.random_mouse_movements(page)
        html = page.content().lower()
        page.mouse.click(1, 1)
        time.sleep(1) 
        if 'go to home' in html or 'full site'in html or 'href="/home"' in html:
            list_mhtml_traffic = self.go_to_home_page(page, list_mhtml_traffic)
            go_to_home_page = True
        else:
            print('the page is in home page')
            go_to_home_page = False
        return list_mhtml_traffic, go_to_home_page

    def go_to_home_page(self, page, list_mhtml_traffic):
        try:
            self.random_mouse_movements(page)
            list_home = []
            self.__logger.info(f'--try to go to home page --')
            list_a = page.locator("a").all()
            for num ,elem in enumerate(list_a):
                try:
                    href = elem.get_attribute("href")
                    if '/home' in href:
                        list_home.append(elem)
                except:
                    pass
            if len(list_home)<7:

                try:
                    self.random_mouse_movements(page)
                    page.mouse.click(1, 1)
                    page.mouse.click(2, 2)
                except:
                    pass
                time.sleep(1)

                tries = 1
                while tries < 4:
                    try:
                        random_element = random.choice(list_home)
                        # self.__logger.info(f'clikling on element {random_element}')
                        page.mouse.click(1, 2)
                        self.random_mouse_movements(page)
                        # random_element.click(timeout=10000)
                        # page.wait_for_load_state(timeout=20000)
                        self.move_and_click(random_element,page)
                        page.wait_for_selector("body", timeout=15000)
                        break
                    except Exception as e:
                        self.__logger.error(f'click home page {e} trie {tries}')
                        tries += 1
            else:
                self.__logger.info('the page is in home page')
            return list_mhtml_traffic 

        except Exception as e:
            self.__logger.error(f'Playwright_automation::go_to_home_page -  error {e}')

    def go_to_movie(self, page, list_mhtml_traffic):
        try:
            self.__logger.info(f'--try to go to movie --')
            # Desplazarse hacia abajo hasta el final de la página
            previous_height = None
            self.__logger.info('scrolling site')
            count = 1
            while count < 10:
                try:
                    # Desplazarse hacia abajo
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    self.random_mouse_movements(page)
                    # Esperar a que la nueva parte de la página se cargue
                    page.wait_for_timeout(2000)  # Puedes ajustar el tiempo de espera según sea necesario
                    # Verificar la altura de la página
                    current_height = page.evaluate("document.body.scrollHeight")
                    if current_height == previous_height:
                        break
                    previous_height = current_height
                    count = count + 1
                except:
                    count = count + 1
           
            list_movies = self.get_list_element_movie(page)
            if list_movies:
               list_mhtml_traffic =  self.click_with_retry(list_movies, page, list_mhtml_traffic)
            else:
                self.__logger.info(f'--no movies found --')
            return list_mhtml_traffic

        except Exception as e:
            self.__logger.error(f'Playwright_automation::go_to_movie -  error {e}')
            return list_mhtml_traffic

    def click_with_retry(self, list_element, page, list_mhtml_traffic):
        try:
            self.__logger.info(f'--try to click on movie --')
            viewport_size = page.viewport_size
            tries = 3
            trie = 1
            while trie < tries: 
                self.__logger.info(f'--try {trie} --')            

                try:
                    self.random_mouse_movements(page)
                    page.mouse.click(1, 1)
                    page.mouse.click(2, 2)
                except:
                    pass
                random_element = random.choice(list_element)

                self.__logger.info(f'--try to click on {random_element} --')
                """ try:
                    random_element.scroll_into_view_if_needed()
                    self.random_mouse_movements(page)
                    random_element.click(timeout=20000)
                    self.__logger.info(f'--waiting load movie --')
                    # page.wait_for_load_state(timeout=20000)
                    page.wait_for_selector("body", timeout=15000)
                    break
                except Exception as e:
                    self.__logger.error(f'--error click on  {random_element} : {e} --')
                    trie += 1 """
                try:
                    random_element.scroll_into_view_if_needed()
                    self.random_mouse_movements(page)  # Más movimientos antes del clic
                    
                    """ # 🖱️ Obtener coordenadas del elemento
                    box = random_element.bounding_box()
                    if box:
                        x = box["x"] + (box["width"] / 2)  # Centro horizontal
                        y = box["y"] + (box["height"] / 2)  # Centro vertical

                        # 🏃‍♂️ Mover el mouse de manera suave al elemento
                        steps = random.randint(5, 15)  # Número de pasos aleatorios
                        page.mouse.move(x, y, steps=steps)

                        time.sleep(random.uniform(0.3, 1.2))  # Pausa natural antes del clic

                        # 🔘 Clic en el elemento
                        page.mouse.click(x, y) """

                    self.move_and_click(random_element, page)  
                    self.__logger.info(f'--waiting load movie --')
                    page.wait_for_selector("body", timeout=15000)
                    break

                except Exception as e:
                    self.__logger.error(f'--error click on  {random_element} : {e} --')
                    trie += 1

            return list_mhtml_traffic
                               
        except Exception as e:
            self.__logger.error(f'Error on click_with_retry: {e}')
            
    def get_list_element_movie(self, page):
        href_elem_list = []
        try:
            # Utiliza el locator para obtener todas las etiquetas <a>
            anchor_locator = page.locator("a").all()
            current_url = page.url
            domain_url_page = re.findall(r'https?:\/\/([^\/]+)', current_url)[0]

            # Itera sobre los elementos y obtén los atributos href
            for anchor in anchor_locator:
                try:
                    href = anchor.get_attribute("href")
                    if domain_url_page in href or 'watch' in href:
                        href_elem_list.append(anchor)
                except Exception as e:
                    pass
        except Exception as e:
            self.__logger.error(f'-- get_list_element_movie : {e}---')
            pass
        return href_elem_list
    
    def move_and_click(self, element, page):
        try:
            self.__logger.info(f'moving and clicking on {element}')
            box = element.bounding_box()
            if box:
                x = box["x"] + (box["width"] / 2)  # Centro horizontal
                y = box["y"] + (box["height"] / 2)  # Centro vertical

                # 🏃‍♂️ Mover el mouse de manera suave al elemento
                steps = random.randint(5, 15)  # Número de pasos aleatorios
                page.mouse.move(x, y, steps=steps)

                time.sleep(random.uniform(0.3, 1.2))  # Pausa natural antes del clic

                # 🔘 Clic en el elemento
                # page.mouse.click(x, y)
                click_try = 1
                while click_try < 4 :
                    try:
                        element.click(timeout=20000)
                        self.__logger.info(f'click succesfull' )
                        break
                    except Exception as e:
                        self.__logger.error(f'click failed' )
                        click_try = click_try + 1

            else:
                element.click(timeout=20000)


        except Exception as e:
            self.__logger.error(f' move_and_click - {e}')
    
    def human_type(page, selector, text):
        for char in text:
            page.locator(selector).press(char)
            time.sleep(random.uniform(0.05, 0.2))  # Simula el tiempo entre pulsaciones

    def random_mouse_movements(self, page):
        try:
            width, height = page.evaluate("() => [window.innerWidth, window.innerHeight]")
            for _ in range(random.randint(5, 15)):  
                x, y = random.randint(0, width), random.randint(0, height)
                page.mouse.move(x, y, steps=random.randint(3, 10))
                time.sleep(random.uniform(0.1, 0.5)) 
        except Exception as e:
            self.__logger.error('error on random mouse movement')  
    
    # def get_list_elemet_movie(self, page):
    #     self.__logger.info(f'--get list element movie --')
    #     try:
    #         elements = page.get_by_role('link').all()
    #         current_url = page.url        
    #         domain_url_page = re.findall(r'https?:\/\/([^\/]+)', current_url)[0] 
            
    #         for element in elements:
    #             handle = element.element_handle()
    #             text = handle.inner_text().lower()
    #             if domain_url_page in text or 'watch' in text:
    #                 found_flag = True
    #                 try:
    #                     element.click()
    #                     self.__logger.info(f'click successful on {text}')
    #                     break
    #                 except Exception as e:
    #                     self.__logger.error(f'error click on {text} - error {e}')
    #         return list_movies
    #     except Exception as e:
    #         self.__logger.error(f'Playwright_automation::get_list_elemet_movie -  error {e}')
    #         return None
    
        
        

