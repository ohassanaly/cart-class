/* concatenate UCD and CCAM retrieved patients   */

data work.cart_df;
    set work.TEST2 work.FULLUCDPMSI_TEMP;
run;

/* merge main table with IR_BEN_R (BEN_DCD_DTE for survival analysis)*/

proc sql;
    create table SASDATA1.CART_VITAL as
    select a.*, 
           b.*
    from work.cart_df a
    left join ORAVUE.IR_BEN_R b
        on a.NIR_ANO_17 = b.BEN_IDT_ANO;
quit;