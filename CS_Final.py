from AB_Final import RDBMS
from CD_Final import HIMT
from Data_visualisations import Viz
import time
import sys

# sys.path.append('/Users/KXC/anaconda3/lib/python3.7/site-packages/pyspark')
# import os; os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12-2.4.0 pyspark-shell'


def run_jobs(cart,lib, gcart):

    for i in cart:
        if cart[i] == "table":
            RDBMS(str(i)).transform_data().sendtoMongo()
        elif cart[i] == 'file':
            with HIMT([i]) as I:
                I.publish_subscribe_and_sink()
        else:
            for k, name in gcart.items():
                if name == i:
                    print(f'graphing, {i}')
                    eval('Viz().' + k + '()')
    print('all jobs have been executed.')
    cart.clear()


def parse_input(subnav, nav, view, cart, lib, gcart):
    for i in subnav:
        if i in lib['sys_nav']:
            print(f'\n\n -->{i} passed in... {lib[i]}', end='\n\n')

            if i == 'X':
                print('running all jobs in cart')
                run_jobs(cart, lib, gcart)
                return nav
            elif i == '#':
                if nav == '0':
                    return '0'
                else:
                    new_nav = str(int(nav)-1)
                    return new_nav
            elif i == '*':
                print(f'adding all {view} to job cart, and then running all jobs')
                try:
                    print('lib[view]', lib[view])
                    for i in lib[view]:
                        cart[i] = view
                    run_jobs(cart, lib, gcart)
                    return nav
                except KeyError:
                    print(f'\n\n "*" function not available in {view} mode')
            else:
                return subnav
        if i in lib['tables']:
            print(f'\n adding {lib[i]} to job cart')
            cart[lib[i]] = 'table'
            lib['tables'] = lib['tables'].replace(i, '')
            lib.pop(i)

        elif i in lib['Visualizations']:
            print(f'adding {lib[i]} to job cart')
            gcart[i] = lib[i]
            cart[(lib[i])] = 'graph'
            lib['Visualizations'] = lib['Visualizations'].replace(i, '')
            lib.pop(i)
        elif i in lib['files']:
            print(f'adding {lib[i]} to job cart')
            cart[lib[i]] = 'file'
            lib['files'] = lib['files'].replace(i, '')
            lib.pop(i)
        else:
            print(f'{i} is not a valid entry', end='\n\n')
    else:
        return '0'


def showcart(cart):
    if len(cart) == 0:
        print('\nYour cart is currently empty.')
    else:
        print('\n Here is your current cart:')
        for idx, i in enumerate(cart, start=1):
            print(f'\t{idx}) {i} ({cart[i]})')


def main():
    lib = {
        'tables': 'abc',
        'a': 'cdw_sapp_branch', 
        'b': 'cdw_sapp_creditcard', 
        'c': 'cdw_sapp_customer',

        'files': 'defghijk',
        'd': 'BenefitsCostSharing_partOne.txt',
        'e': 'BenefitsCostSharing_partTwo.txt',
        'f': 'BenefitsCostSharing_partThree.txt',
        'g': 'BenefitsCostSharing_partFour.txt',

        'h': 'Network.csv',
        'i': 'ServiceArea.csv',
        'j': 'insurance.txt',
        'k': 'PlanAttributes.csv',

        'sys_nav': "*&1230#X",
        '0': 'moving to Master View',
        '1': 'moving to Migrations page',
        '2': 'moving to NoSQL page',
        '3': 'Moving to Visualizations page',
        '#': 'Backing out one level',
        '&': 'logging out now',
        '*': 'running all within View',
        'X': 'Starting job list',

        'Visualizations': 'ABDEF',
        'A': 'Bar chart of ServiceAreaName, SourceName, and Business Year across each state',
        'B': 'Bar chart of country of "source" names in the US',
        'D': 'Bar chart of number of benefit plans in each state',
        'E': 'Show the number of the mothers who smoke within the insurance dataset',
        'F': 'Visualization showing which region has the highest rate of smokers'   
    }

    gcart = {}
    cart = {}
    print("\n\nWelcome to the data warehouse interface!\n\n\n")
    time.sleep(2)
    print('''This programs batches work. Both from an input, and output basis.
            This means you may input multiple letters at a time. \n
            Once your ready execute your task(s) input "X" within the command line, 
            and the program will complete all the task currently within the cart''')
    time.sleep(2.7)

    print('''\nPlease take note of the following: \n\t

            1) jobs migrated previously within the session will not be available
            for a second migration\n\t

            2) inputs will be evaluated in the order they are input\n\n
            Now... Let's begin by showing you all the contents within the program\n''')

    input("Press enter to continue")
    print('\n\n')

    print('1. Migration\n')
    for idx, l in enumerate('abcdefghikABDEF'):
        print(f'\t {l}) {lib[l]}')
        if idx == 2 or idx == 6:
            print()
        if idx == 9:
            print('\n2. Visualizations\n')

    nav = input('''
    See above for a list of all options within the program
    Input a 1, 2, or 3 for views of the RDBMS data, URL NoSQL files, and the Visualizations respectively.
    (for the visualizations to run successfully, make sure you've migrated the data previously, or just added the data within the cart to ensure output is created)

    Input "0" will take you to the table of contents \n

    Press 0-3 to see a view and then start adding to you job cart or any other input to simply move foward in the program
    you may force quit the application by entering '&' \n ''')

    view = 'total view'
    while nav != '&':

        t = {'total view': lib['tables']+lib['files']+lib['Visualizations']}

        if nav == '0':
            print('\n\nHere is the master table of contents:\n\n')
            view = 'total view'
            for i in t['total view']:
                print(f'\t{i}) {lib[i]}')
            print('\n')

        elif nav == '1':
            print('\n', '*'*30, '\n MariaDB tables\n', '*'*30)
            view = 'tables'
            for i in lib['tables']:
                print(f'{i}) {lib[i]}')
        elif nav == '2':
            print('\n', "*"*25, '\n Files for extraction, transformation, and loading\n', "*"*25)
            view = 'files'
            for i in lib['files']:
                print(f'{i}) {lib[i]}')

        elif nav == '3':
            print('*'*30, 'Visualizations', "*"*30, sep='\n')
            view = 'Visualizations'
            for i in lib['Visualizations']:
                print(f'{i}) {lib[i]}')

        showcart(cart)
        sub_nav = input('''\n
                Input the corresponding letter(s) followed by enter to one or more multiple tasks to your cart.
                You may input '*' to multiple the tables to the cart at once.
                input '#' to go back a tree (if you're on the master page it will replay the master page \n
                note: this input can take multiple letter arguments.
                You can put any combination of valid inputs in this part of the program
                \n''')
        nav = parse_input(sub_nav, nav, view, cart, lib, gcart)

    print('\n Logout successful. Program closed')



if __name__ == '__main__':
    main()
