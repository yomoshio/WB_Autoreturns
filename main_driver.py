import asyncio
import subprocess
import sys
import gc
from multiprocessing import Process

from driver import main as driver_main
from fixing import main as fixing_main
from excluded import main as excluded_main
from updated import main as updated_main
from clear_all import main as clear_all_main
from spliting import main as main_spliting
from generate_stop import main as stop_main
from get_google import main as google_main
from stocks_wb import main as stocks_main


def run_subprocess(script_name, *args):
    """
    Запускает скрипт через subprocess с xvfb-run.
    """
    try:
        print(f"Запуск {script_name}...")
        subprocess.run(["xvfb-run", "-a", sys.executable, script_name, *args], check=True)
        print(f"{script_name} завершен.")
    except subprocess.CalledProcessError as e:
        print(f"Ошибка при выполнении {script_name}: {e}", file=sys.stderr)
        sys.exit(1)


def run_scripts(cab_name, cab_pref, user_data_dir):
    """
    Запуск всех скриптов для одного кабинета.
    """
    try:        
        print(f"Запуск clear_all.py для {cab_name}...")
        clear_all_main(cab_pref=cab_pref)
        print(f"clear_all.py завершен для {cab_name}.")
        gc.collect()  # Освобождаем память

        print(f"Запуск driver.py для {cab_name}...")
        driver_main(cab_pref=cab_pref)
        print(f"driver.py завершен для {cab_name}.")
        gc.collect()

        print(f"Запуск get_google.py для {cab_name}...")
        google_main(cabinet_name=cab_name, cab_pref=cab_pref)
        print(f"get_google.py завершен для {cab_name}.")
        gc.collect()

        print(f"Запуск fixing.py для {cab_name}...")
        fixing_main(cab_pref=cab_pref)
        print(f"fixing.py завершен для {cab_name}.")
        gc.collect()

        print(f"Запуск generate_stop.py для {cab_name}...")
        stop_main(cab_pref=cab_pref)
        print(f"generate_stop.py завершен для {cab_name}.")
        gc.collect()

        print(f"Запуск spliting.py для {cab_name}...")
        main_spliting(cab_pref=cab_pref)
        print(f"spliting.py завершен для {cab_name}.")
        gc.collect()

        print(f"Запуск wb_stocks.py и excluded.py для {cab_name}...")
        asyncio.run(stocks_main(cab_pref=cab_pref))
        asyncio.run(excluded_main(cab_pref=cab_pref))
        print(f"wb_stocks.py и excluded.py завершены для {cab_name}.")
        gc.collect()

        print(f"Запуск updated.py через xvfb-run для {cab_name}...")
        run_subprocess("updated.py", cab_pref, user_data_dir)

        print(f"Все скрипты успешно выполнены для {cab_name}.")
    except Exception as e:
        print(f"Ошибка при выполнении скриптов для {cab_name}: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    params1 = ("SHOP_NAME", "B", "SESSION_PATH")
    params2 = ("SHOP_NAME", "T", "SESSION_PATH")

    process1 = Process(target=run_scripts, args=params1)
    process2 = Process(target=run_scripts, args=params2)

    process1.start()
    process2.start()

    process1.join()
    process2.join()

    print("Все процессы завершены.")


if __name__ == "__main__":
    main()
