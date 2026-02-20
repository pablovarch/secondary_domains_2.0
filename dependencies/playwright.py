from dependencies import log, Playwright_traffic
import constants, settings
import json
import io

from playwright.sync_api import Playwright, sync_playwright, BrowserType


class Playwright:
    def __init__(self):
        self.__logger = log.Log().get_logger(name=constants.log_file['log_name'])

    def navigation(self, domain_item, proxy_dict_playwright, random_profile):
        self.__logger.info(" --- set playwright chromium ---")

        use_proxy = getattr(settings, "proxy", False)
        proxy_dict = proxy_dict_playwright if use_proxy else None

        browser = None
        context = None
        page = None

        try:
            with sync_playwright() as p:
                try:
                    # ✅ Proxy GLOBAL en launch (si aplica)
                    launch_kwargs = {"channel": "msedge", "headless": False}
                    if proxy_dict is not None:
                        launch_kwargs["proxy"] = proxy_dict

                    browser = p.chromium.launch(**launch_kwargs)

                    # ✅ Contexto limpio (sin persistencia)
                    context = browser.new_context()

                    page = context.new_page()
                    page.set_viewport_size({"width": 1920, "height": 1080})

                    # Captura el tráfico de red y procesa las solicitudes HTTP
                    status_dict, dict_feature_domain, html_features = (
                        Playwright_traffic.Playwright_traffic().capture_traffic(page, domain_item)
                    )

                    # Obtener el tamaño total de la página en píxeles
                    html_length = None
                    try:
                        page_size = page.evaluate(
                            """() => {
                                return {
                                    width: document.documentElement.scrollWidth,
                                    height: document.documentElement.scrollHeight
                                };
                            }"""
                        )
                        self.__logger.info(f'Tamaño de la página: {page_size["width"]}x{page_size["height"]} píxeles')
                        html_length = page_size["height"]
                    except Exception:
                        html_length = None

                    return status_dict, dict_feature_domain, html_features, html_length

                except PlaywrightTimeoutError as e:
                    self.__logger.exception(
                        f"[PlaywrightTimeoutError] Timeout en navigation domain_item={domain_item}: {e}")
                    raise
                except PlaywrightError as e:
                    self.__logger.exception(
                        f"[PlaywrightError] Error Playwright en navigation domain_item={domain_item}: {e}")
                    raise
                except Exception as e:
                    self.__logger.exception(
                        f"[Exception] Error inesperado en navigation domain_item={domain_item}: {e}")
                    raise

                finally:
                    # ✅ Cierre seguro
                    if page is not None:
                        try:
                            page.close()
                        except Exception:
                            pass
                    if context is not None:
                        try:
                            context.close()
                        except Exception:
                            pass
                    if browser is not None:
                        try:
                            browser.close()
                        except Exception:
                            pass

        except Exception:
            raise

    def save_mhtml(path: str, text: str):
        with open(path, mode='w', encoding='UTF-8', newline='\n') as file:
            file.write(text)