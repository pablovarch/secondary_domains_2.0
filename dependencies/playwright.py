from dependencies import log, Playwright_traffic
import constants, settings
import json
import io

from playwright.sync_api import Playwright, sync_playwright, BrowserType


class Playwright:
    def __init__(self):
        self.__logger = log.Log().get_logger(name=constants.log_file['log_name'])

    def navigation(self, domain_item, proxy_dict_playwright, random_profile):
        try:
            self.__logger.info(
                f" --- set playwright chromium ---")

            # Inicializa Playwright y crea un contexto
            with sync_playwright() as p:
                pw: Playwright = p
                browser = pw.chromium.launch(channel="msedge", headless=False)
                proxy_dict = None
                if settings.proxy:
                        proxy_dict=proxy_dict_playwright
                context = p.chromium.launch_persistent_context(


                    user_data_dir=settings.path_user_data_dir,
                    headless=False,

                    channel="msedge",

                    args=[
                        # f"--disable-extensions-except={constants.path_to_extension}",
                        # f"--load-extension={constants.path_to_extension}",
                        # f'--profile-directory={constants.user_profile}'
                        f'--profile-directory={random_profile}',
                        "--start-fullscreen"
                    ],
                    
                    proxy = proxy_dict,

                )
                page = context.new_page()
                page.set_viewport_size({"width": 1920, "height": 1080})
                

                # Captura el tráfico de red y procesa las solicitudes HTTP
                try:

                    status_dict , dict_feature_domain, html_features = Playwright_traffic.Playwright_traffic().capture_traffic(page, domain_item)
                    try:
                        # Obtener el tamaño total de la página en píxeles
                        page_size = page.evaluate('''() => {
                                                                           return {
                                                                               width: document.documentElement.scrollWidth,
                                                                               height: document.documentElement.scrollHeight
                                                                           };
                                                                       }''')

                        self.__logger.info(f'Tamaño de la página: {page_size["width"]}x{page_size["height"]} píxeles')
                        html_length = page_size["height"]
                    except Exception as e:
                        html_length = None

                    # Cierra el navegador y el contexto                    
                    page.close()
                    context.close()
                    browser.close()
                    return status_dict , dict_feature_domain, html_features , html_length
                except Exception as e:
                    list_ad_chains_url = []
                    raise
        except Exception as e:
            # self.__logger.error(f'error on Navigation - create driver - error {e}')
            raise
        
        
    def save_mhtml(path: str, text: str):
        with open(path, mode='w', encoding='UTF-8', newline='\n') as file:
            file.write(text)
    



