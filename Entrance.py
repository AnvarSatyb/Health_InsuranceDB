def main():
    print('Hello. How can we help you today?')
    entry = None
    while entry != 'd':
        entry = input('\na) If you would like to see credit card data please press (a)\
                    \nb) If you would like to see Health Insurance data please press (b)\
                    \nc) If you would like to visualize or analyze uploaded data please press (c)\
                    \nd) Otherwise please press (d), and you will be logged out from the system\
                    \n\nPlease choose one of the following options a, b, c, or d:\
                    \n-------- ')
        if entry =='a':
            abcd = input('\n1.a) If you would like to see branch data please press (a)\
                    \n2.b) If you would like to see credit card data please press (b)\
                    \n3.c) If you would like to see customers data please press (c)\
                    \n4.d) Otherwise please press (d), and you will return to the previous menu\
                    \n-------- ')
            if abcd =='a':
                from case_study_spark import A1
            elif abcd=='b':
                from case_study_spark import A2
            elif abcd=='c':
                from case_study_spark import A3  
            else:
                print('You are returned to main menu')
        elif entry =='b':
            abcd = input('\nHealth Insurance Marketplace Data Files:\
                           \n\t2.a) Benefits Cost Sharing\
                           \n\t2.b) Insurance\
                           \n\t2.c) PlanAttributes\
                           \n\t2.d) Network\
                           \n\t2.e) ServiceArea\
                           \n\t Otherwise please press any button, and you will return to the previous menu\
                           \n\nPlease select anything from the list\
                           \n----------- ')
            if abcd == 'a':
                abcd = input('\nBenefits CostSharing Data Files:\
                           \n\t2.a.a) BenefitsCostSharing part 1\
                           \n\t2.a.b) BenefitsCostSharing part 2\
                           \n\t2.a.c) BenefitsCostSharing part 3\
                           \n\t2.a.d) BenefitsCostSharing part 4\
                           \n\t Otherwise please press any button, and you will return to the main menu\
                           \n\nPlease select anything from the list\
                           \n----------- ')
                if abcd == 'a':
                    from case_study_spark import B1
                elif abcd =='b':
                    from case_study_spark import benef2
                elif abcd =='c':
                    from case_study_spark import benef3            
                elif abcd =='d':
                    from case_study_spark import benef4            
                else:
                    print('You are returned to main menu')            
            elif abcd =='b':
                from case_study_spark import B2
            elif abcd =='c':
                from case_study_spark import B3            
            elif abcd =='d':
                from case_study_spark import B4            
            elif abcd =='e':
                from case_study_spark import B5
            else:
                print('You are returned to main menu')
        elif entry =='c':            
            abc = input('Choose on of the following options:\
            \n\t3.a) Here you can see counts of ServiceAreaName, SourceName, and BusinessYear by state\
                           \n\t3.b) Here you can see the counts of sources across the country\
                           \n\t3.c) Here you can see table of the names of the plans with the most customers by state\
                           \n\t3.d) Here you can see the number of benefit plans in each state\
                           \n\t3.e) Here you can see quantity of smoking mother\
                           \n\t3.f) Here you can see highest smoking region\
                           \n\t Otherwise please press any button, and you will return to the previous menu\
                           \n\nPlease select an option from the list\
                           \n------- ')
            if abc == 'a':
                from case_study_spark import C_A
            elif abc =='b':
                from case_study_spark import C_B
            elif abc =='c':
                from case_study_spark import C_C    
            elif abc =='d':
                from case_study_spark import C_D            
            elif abc =='e':
                from case_study_spark import C_E            
            elif abc =='f':
                from case_study_spark import C_F
            else:
                print('You are returned to main menu')

    print('System is closed now. See you soon')
                                
if __name__=='__main__':
    main()