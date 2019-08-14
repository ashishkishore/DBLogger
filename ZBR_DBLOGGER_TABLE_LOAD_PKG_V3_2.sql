CREATE OR REPLACE PACKAGE ZBR_DBLOGGER_TABLE_LOAD_PKG IS

  FUNCTION ZBR_DEPOT_LOC_LKP(F_LOC IN VARCHAR2) RETURN VARCHAR2;
  FUNCTION GET_AUDIT_TRAIL_LOG(P_CIKEY IN INTEGER) RETURN BLOB;
  FUNCTION ZBR_GET_AUDIT_PAYLOAD(L_CUBE_CIKEY VARCHAR2) RETURN BLOB;
  FUNCTION ZBR_DBLOGGER_TIMESTAMP(F_TS IN VARCHAR2) RETURN TIMESTAMP;
  FUNCTION ZBR_DBLOGGER_TZ_CONV(F_TS IN VARCHAR2)	RETURN VARCHAR2;
  PROCEDURE ZBR_DBLOGGER_TMP_TAB_LOAD_SAN(P_STATUS OUT VARCHAR2);
  PROCEDURE ZBR_DBLOGGER_TMP_TAB_LOAD_DBK(P_STATUS OUT VARCHAR2);
  PROCEDURE ZBR_DBLOGGER_TABLE_LOAD_SAN(P_STATUS OUT VARCHAR2);
  PROCEDURE ZBR_DBLOGGER_TABLE_LOAD_DBK(P_STATUS OUT VARCHAR2);
  PROCEDURE ZBR_DBLOGGER_TABLE_PURGE_PROC(P_PURGE_DAYS IN NUMBER,P_STATUS  OUT VARCHAR2);
  PROCEDURE ZBR_DBLOGGER_INSERT(TAB_LIST IN ZBR_DBLOGGER_TAB_LIST,P_STATUS OUT VARCHAR2,P_MSG OUT VARCHAR2);
  PROCEDURE ZBR_DBLOGGER_VB_FETCH(P_IN_VALUE IN ZBR_DBLOGGER_VB_IP_TAB_REC,P_OUT_VALUE OUT ZBR_DBLOGGER_VB_OP_TAB_LIST,P_STATUS OUT VARCHAR2,P_MSG OUT VARCHAR2);

 END ZBR_DBLOGGER_TABLE_LOAD_PKG;
/
CREATE OR REPLACE PACKAGE BODY ZBR_DBLOGGER_TABLE_LOAD_PKG IS

/*===================================================================================================================================
   The purpose of this code is to build data load Functionality for DB Logger table.

    DEVELOPED BY:   PRIYANKA JAYAVEL    VERSION    1.0     DATE: 08/05/2018 1131 PM
	DEVELOPED BY:   ASHISH KUMAR    	VERSION    2.0     DATE: 07/03/2019 0700 PM  Made the fix after modifying Control table column's data type (From_Date and Curr_date)
	DEVELOPED BY:   ASHISH KUMAR    	VERSION    3.0     DATE: 10/03/2019 0230 PM  Added ZBR_DBLOGGER_INSERT proc and ZBR_DEPOT_LOC_LKP function for loading data into DBLOGGER table through OIC.
	DEVELOPED BY:   ASHISH KUMAR    	VERSION    3.1     DATE: 15/03/2019 0755 PM  Added ZBR_DBLOGGER_VB_FETCH proc and ZBR_DBLOGGER_TIMESTAMP function for fetching data from DBLOGGER table.
	DEVELOPED BY:   ASHISH KUMAR    	VERSION    3.2     DATE: 26/03/2019 0856 PM  Added ZBR_DBLOGGER_TZ_CONV Function.
=====================================================================================================================================*/

  FUNCTION get_audit_trail_log(p_cikey IN INTEGER) RETURN blob IS
    bl BLOB;
  BEGIN
    dbms_lob.createtemporary(bl, TRUE);
    FOR r_log IN (select log
                    from TSO_SOAINFRA.audit_trail
                   where cikey = p_cikey
                   order by count_id) LOOP
      dbms_lob.append(bl, r_log.log);
    END LOOP;
    --
    RETURN(bl);
  END;

  function ZBR_GET_AUDIT_PAYLOAD(l_cube_cikey varchar2) return blob is
    l_audit_payload blob;
  begin
    l_audit_payload := null;
    select get_audit_trail_log(l_cube_cikey) PAYLOAD
      into l_audit_payload
      from dual;
    return l_audit_payload;
  exception
    when others then
      return null;
  end;

  Function ZBR_DEPOT_LOC_LKP(F_LOC IN VARCHAR2) RETURN VARCHAR2
 AS
 reg VARCHAR2(100);
    BEGIN
        SELECT region INTO reg FROM zeb_tso_depot_lkp WHERE depot_loc=f_loc;
        RETURN reg;
    EXCEPTION
        WHEN no_data_found THEN
        RETURN NULL;
  END;
  
  FUNCTION ZBR_DBLOGGER_TIMESTAMP(F_TS IN VARCHAR2)
	RETURN TIMESTAMP
	AS
	BEGIN
	RETURN TO_TIMESTAMP(F_TS,'DD-MON-YY HH.MI.SS.FF AM');
	EXCEPTION
    WHEN OTHERS THEN 
    RETURN NULL;
  END ZBR_DBLOGGER_TIMESTAMP;
  
  FUNCTION ZBR_DBLOGGER_TZ_CONV(F_TS IN VARCHAR2)
	RETURN VARCHAR2
	AS
	BEGIN
	RETURN TO_CHAR(FROM_TZ(ZBR_DBLOGGER_TIMESTAMP(F_TS),'UTC') AT TIME ZONE 'AMERICA/CHICAGO','DD-MON-YY HH.MI.SS.FF AM');
	EXCEPTION
    WHEN OTHERS THEN 
    RETURN NULL;
  END ZBR_DBLOGGER_TZ_CONV;
 
  procedure ZBR_DBLOGGER_TMP_TAB_LOAD_DBK(p_status out varchar2) is

    cursor main_cur(p_from_date date, p_to_date date) is
      SELECT sftc.composite_id comp_id,
             NVL(sftc.title, sfi.title) comp_title,
             ci.cikey cube_cikey,
             se.composite comp_composite_dn,
             se.composite cube_composite_name,
             sftc.CREATED_TIME creation_date,
             sftc.UPDATED_TIME modify_date,
             (SELECT region
                FROM zbrsoausr.zeb_tso_depot_lkp
               WHERE NVL(sftc.title, sfi.title) LIKE '%' || depot_loc || '%'
                 AND ROWNUM < 2) region,
             ZBR_GET_AUDIT_PAYLOAD(ci.cikey) audit_payload
        FROM tso_soainfra.sca_flow_to_cpst  sftc,
             tso_soainfra.sca_entity        se,
             tso_soainfra.cube_instance     ci,
             tso_soainfra.sca_flow_instance sfi
       WHERE sftc.composite_sca_entity_id = se.id(+)
         and sftc.composite_id = ci.cmpst_id(+)
         and sftc.flow_id = ci.flow_id(+)
         AND sftc.flow_id = sfi.flow_id
         AND se.composite IN ('ZEBRepairOutboundProvServiceImpl',
              'ZEBRepairInboundProvServiceImpl')
         and NVL(sftc.title, sfi.title) is not null
         and sftc.UPDATED_TIME >= p_from_date
         and sftc.UPDATED_TIME <= p_to_date;
         --and sfi.flow_id = '680005';

    TYPE main_rec_type IS TABLE OF main_cur%ROWTYPE;
    l_rec_type  main_rec_type := main_rec_type();
    l_from_date date;
    l_to_date   date;
    l_temp      varchar2(500);

  begin
    EXECUTE IMMEDIATE 'TRUNCATE TABLE ZBRSOAUSR.zeb_tso_dblogger_temp';
    --Get control table dates
    BEGIN
      select TO_DATE(CONTROL_DATE_FROM,'DD-MON-YYYY'), nvl(CONTROL_DATE_CURR, sysdate)
        into l_from_date, l_to_date
        from ZBRSOAUSR.ZEB_SOA_CONTROL_TABLE
       WHERE PROCESS_NAME = 'DBK_LOAD';
    EXCEPTION
      when others then
        l_from_date := null;
        l_to_date   := null;

    END;

    OPEN main_cur(l_from_date, l_to_date);

    LOOP
      FETCH main_cur BULK COLLECT
        INTO l_rec_type LIMIT 1000;
      BEGIN
        FORALL i in 1 .. l_rec_type.COUNT
          insert into zbrsoausr.zeb_tso_dblogger_temp
            (record_id,
             system_name,
             instance_id,
             comp_title,
             title,
             cube_cikey,
             comp_composite_dn,
             process_name,
             creation_date,
             modify_date,
             record_status,
             audit_payload_blob,
             region)
          values
            (zeb_tso_dblog_seq.nextval,
             'DBK',
             l_rec_type(i).comp_id,
             l_rec_type(i).comp_title,
             l_rec_type(i).comp_title,
             l_rec_type(i).cube_cikey,
             l_rec_type(i).comp_composite_dn,
             l_rec_type(i).cube_composite_name,
             l_rec_type(i).creation_date,
             l_rec_type(i).modify_date,
             'Partially Loaded',
             l_rec_type(i).audit_payload,
             l_rec_type(i).region);

        commit;
      EXCEPTION
        WHEN OTHERS THEN
          l_temp := 'DBK Exception1-' || SQLERRM;
          insert into dblogger_error_tab
          values
            (ZEB_TSO_DBLOG_ERR_SEQ.nextval, l_temp, sysdate);
          commit;
      END;
      EXIT WHEN main_cur%NOTFOUND;
    end loop;
    commit;
    --Updating control table
    BEGIN
      update ZBRSOAUSR.ZEB_SOA_CONTROL_TABLE
         set CONTROL_DATE_FROM = TO_CHAR(l_to_date,'DD-MON-YYYY'), CONTROL_DATE_CURR = null
       WHERE PROCESS_NAME = 'DBK_LOAD';
      commit;
    EXCEPTION
      when others then
        l_temp := 'DBK Exception2-' || SQLERRM;
        insert into dblogger_error_tab
        values
          (ZEB_TSO_DBLOG_ERR_SEQ.nextval, l_temp, sysdate);
        commit;
    END;
    --run gather stats for the staging table
    BEGIN
      DBMS_STATS.gather_table_stats(ownname       => 'ZBRSOAUSR',
                                    tabname       => 'zeb_tso_dblogger_temp',
                                    no_invalidate => FALSE);

    EXCEPTION
      when others then
        l_temp := 'DBK Exception3-' || SQLERRM;
        insert into dblogger_error_tab
        values
          (ZEB_TSO_DBLOG_ERR_SEQ.nextval, l_temp, sysdate);
        commit;
    END;
    p_status := 'SUCCESS';
  exception
    when others then
      p_status := 'ERROR';
      DBMS_OUTPUT.put_line(SQLERRM);
  end ZBR_DBLOGGER_TMP_TAB_LOAD_DBK;

  procedure ZBR_DBLOGGER_TMP_TAB_LOAD_SAN(p_status out varchar2) is

    cursor main_cur(p_from_date date, p_to_date date) is
      SELECT sftc.composite_id comp_id,
             NVL(sftc.title, sfi.title) comp_title,
             ci.cikey cube_cikey,
             se.composite comp_composite_dn,
             se.composite cube_composite_name,
             sftc.CREATED_TIME creation_date,
             sftc.UPDATED_TIME modify_date,
             (SELECT region
                FROM zbrsoausr.zeb_tso_depot_lkp
               WHERE NVL(sftc.title, sfi.title) LIKE '%' || depot_loc || '%'
                 AND ROWNUM < 2) region,
             ZBR_GET_AUDIT_PAYLOAD(ci.cikey) audit_payload
        FROM tso_soainfra.sca_flow_to_cpst  sftc,
             tso_soainfra.sca_entity        se,
             tso_soainfra.cube_instance     ci,
             tso_soainfra.sca_flow_instance sfi
       WHERE sftc.composite_sca_entity_id = se.id(+)
         and sftc.composite_id = ci.cmpst_id(+)
         and sftc.flow_id = ci.flow_id(+)
         AND sftc.flow_id = sfi.flow_id
         AND se.composite IN ('ZEBTSORMAProvServiceImpl',
              'ZEBTSONotificSiebelProvServiceImpl',
              'ZEBTSONotificHoldSiebelProvServiceImpl')
         and NVL(sftc.title, sfi.title) is not null
         and sftc.UPDATED_TIME >= p_from_date
         and sftc.UPDATED_TIME <= p_to_date;
         --and 1=2;

    TYPE main_rec_type IS TABLE OF main_cur%ROWTYPE;
    l_rec_type  main_rec_type := main_rec_type();
    l_from_date date;
    l_to_date   date;
    l_temp      varchar2(500);

  begin
    EXECUTE IMMEDIATE 'TRUNCATE TABLE ZBRSOAUSR.zeb_tso_dblogger_temp';
    --Get control table dates
    BEGIN
      select TO_DATE(CONTROL_DATE_FROM,'DD-MON-YYYY'), nvl(CONTROL_DATE_CURR, sysdate)
        into l_from_date, l_to_date
        from ZBRSOAUSR.ZEB_SOA_CONTROL_TABLE
       WHERE PROCESS_NAME = 'SANMINA_LOAD';
    EXCEPTION
      when others then
        l_from_date := null;
        l_to_date   := null;

    END;

    OPEN main_cur(l_from_date, l_to_date);

    LOOP
      FETCH main_cur BULK COLLECT
        INTO l_rec_type LIMIT 1000;
      BEGIN
        FORALL i in 1 .. l_rec_type.COUNT
          insert into zbrsoausr.zeb_tso_dblogger_temp
            (record_id,
             system_name,
             instance_id,
             comp_title,
             title,
             cube_cikey,
             comp_composite_dn,
             process_name,
             creation_date,
             modify_date,
             record_status,
             audit_payload_blob,
             region)
          values
            (zeb_tso_dblog_seq.nextval,
             'SANMINA',
             l_rec_type(i).comp_id,
             l_rec_type(i).comp_title,
             l_rec_type(i).comp_title,
             l_rec_type(i).cube_cikey,
             l_rec_type(i).comp_composite_dn,
             l_rec_type(i).cube_composite_name,
             l_rec_type(i).creation_date,
             l_rec_type(i).modify_date,
             'Partially Loaded',
             l_rec_type(i).audit_payload,
             l_rec_type(i).region);
        commit;

      EXCEPTION
        WHEN OTHERS THEN
          l_temp := 'Sanmina Exception1-' || SQLERRM;
          insert into dblogger_error_tab
          values
            (ZEB_TSO_DBLOG_ERR_SEQ.nextval, l_temp, sysdate);
          commit;
      END;
      EXIT WHEN main_cur%NOTFOUND;
    end loop;
    commit;
    --Updating control table
    BEGIN
      update ZBRSOAUSR.ZEB_SOA_CONTROL_TABLE
         set CONTROL_DATE_FROM = TO_CHAR(l_to_date,'DD-MON-YYYY'), CONTROL_DATE_CURR = null
       WHERE PROCESS_NAME = 'SANMINA_LOAD';
      commit;
    EXCEPTION
      when others then
        l_temp := 'Sanmina Exception1-' || SQLERRM;
        insert into dblogger_error_tab
        values
          (ZEB_TSO_DBLOG_ERR_SEQ.nextval, l_temp, sysdate);
        commit;
    END;
    --run gather stats for the staging table
    BEGIN
      DBMS_STATS.gather_table_stats(ownname       => 'ZBRSOAUSR',
                                    tabname       => 'zeb_tso_dblogger_temp',
                                    no_invalidate => FALSE);

    EXCEPTION
      when others then
        l_temp := 'Sanmina Exception1-' || SQLERRM;
        insert into dblogger_error_tab
        values
          (ZEB_TSO_DBLOG_ERR_SEQ.nextval, l_temp, sysdate);
        commit;
    END;
    p_status := 'SUCCESS';
  exception
    when others then
      p_status := 'ERROR';
      DBMS_OUTPUT.put_line(SQLERRM);
  end ZBR_DBLOGGER_TMP_TAB_LOAD_SAN;

  procedure ZBR_DBLOGGER_TABLE_LOAD_SAN(p_status out varchar2) is
    l_temp varchar2(500);
  BEGIN
    insert into zbrsoausr.zeb_tso_dblogger
      select *
        from zbrsoausr.zeb_tso_dblogger_temp
       where system_name = 'SANMINA';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE ZBRSOAUSR.zeb_tso_dblogger_temp';
    --run gather stats for the staging table
    BEGIN
      DBMS_STATS.gather_table_stats(ownname       => 'ZBRSOAUSR',
                                    tabname       => 'zeb_tso_dblogger',
                                    no_invalidate => FALSE);

    EXCEPTION
      when others then
        l_temp := 'Exception in DBK ZBR_DBLOGGER_TABLE_LOAD_SAN -' ||
                  SQLERRM;
        insert into dblogger_error_tab
        values
          (ZEB_TSO_DBLOG_ERR_SEQ.nextval, l_temp, sysdate);
        commit;
    END;
    p_status := 'SUCCESS';

  exception
    when others then
      p_status := 'ERROR';
      DBMS_OUTPUT.put_line(SQLERRM);
  end ZBR_DBLOGGER_TABLE_LOAD_SAN;

  procedure ZBR_DBLOGGER_TABLE_LOAD_DBK(p_status out varchar2) is
    l_temp varchar2(500);
  BEGIN
    insert into zbrsoausr.zeb_tso_dblogger
      select *
        from zbrsoausr.zeb_tso_dblogger_temp
       where system_name = 'DBK';

    EXECUTE IMMEDIATE 'TRUNCATE TABLE ZBRSOAUSR.zeb_tso_dblogger_temp';
    --run gather stats for the staging table
    BEGIN
      DBMS_STATS.gather_table_stats(ownname       => 'ZBRSOAUSR',
                                    tabname       => 'zeb_tso_dblogger',
                                    no_invalidate => FALSE);

    EXCEPTION
      when others then
        l_temp := 'Exception in DBK ZBR_DBLOGGER_TABLE_LOAD_SAN -' ||
                  SQLERRM;
        insert into dblogger_error_tab
        values
          (ZEB_TSO_DBLOG_ERR_SEQ.nextval, l_temp, sysdate);
        commit;
    END;
    p_status := 'SUCCESS';
  exception
    when others then
      p_status := 'ERROR';
      DBMS_OUTPUT.put_line(SQLERRM);
  end ZBR_DBLOGGER_TABLE_LOAD_DBK;

   procedure ZBR_DBLOGGER_TABLE_PURGE_PROC(p_purge_days in number,
                                          p_status     out varchar2) is
    l_temp  varchar2(500);
    cursor c_purge(l_purge_days number) is
      select record_id
        from zbrsoausr.zeb_tso_dblogger
       where record_insertion_date <= sysdate - l_purge_days;

    TYPE main_purge IS TABLE OF c_purge%ROWTYPE;
    l_rec_type main_purge := main_purge();
  BEGIN

    OPEN c_purge(p_purge_days);

    LOOP
      FETCH c_purge BULK COLLECT
        INTO l_rec_type LIMIT 10000;
      FORALL i in 1 .. l_rec_type.COUNT
        delete from zbrsoausr.zeb_tso_dblogger
         where record_id = l_rec_type(i).record_id;
    COMMIT;
      EXIT WHEN c_purge%NOTFOUND;
    end loop;

    --run gather stats for the staging table
    BEGIN
      DBMS_STATS.gather_table_stats(ownname       => 'ZBRSOAUSR',
                                    tabname       => 'zeb_tso_dblogger',
                                    no_invalidate => FALSE);

    EXCEPTION
      when others then
        l_temp := 'Exception in ZBR_DBLOGGER_TABLE_PURGE_PROC -' ||
                  SQLERRM;
        insert into dblogger_error_tab
        values
          (ZEB_TSO_DBLOG_ERR_SEQ.nextval, l_temp, sysdate);
        commit;
    END;
    p_status := 'SUCCESS';
  exception
    when others then
      p_status := 'ERROR';
      DBMS_OUTPUT.put_line(SQLERRM);
  end ZBR_DBLOGGER_TABLE_PURGE_PROC;
  
  PROCEDURE ZBR_DBLOGGER_INSERT(TAB_LIST IN ZBR_DBLOGGER_TAB_LIST,P_STATUS OUT VARCHAR2,P_MSG OUT VARCHAR2) AS
L_STATUS VARCHAR2(10);
L_MSG VARCHAR2(4000);
BEGIN
FORALL I IN 1..TAB_LIST.COUNT
INSERT INTO ZEB_TSO_DBLOGGER(
RECORD_ID,
SYSTEM_NAME,
TO_ADDRESS,
EMAIL_SUBJECT,
CONTENT_BODY,
PROCESS_NAME,
PROJECT_NAME,
TITLE,
INSTANCE_ID,
REGION,
DIRECTION,
TRANSACTION_NUMBER,
CREATION_DATE,
MODIFY_DATE,
SR_LINE_NUMBER,
DEPOT_CODE,
TRANSACTION_TYPE,
ERROR_MESSAGE,
SR_NUMBER,
STATUS,
COMP_TITLE,
CUBE_CIKEY,
COMP_COMPOSITE_DN,
SANMINA_PAYLOAD_STR_1,
SANMINA_PAYLOAD_STR_2,
SANMINA_PAYLOAD_STR_3,
SANMINA_PAYLOAD_STR_4,
SIEBEL_PAYLOAD_STR_1,
SIEBEL_PAYLOAD_STR_2,
SIEBEL_PAYLOAD_STR_3,
SIEBEL_PAYLOAD_STR_4,
SIEBEL_ACK_STR,
SANMINA_ACK_STR,
RECORD_INSERTION_DATE,
RECORD_STATUS,
AUDIT_PAYLOAD_BLOB,
ATTRIBUTE1,
ATTRIBUTE2,
ATTRIBUTE3,
ATTRIBUTE4,
ATTRIBUTE5,
ATTRIBUTE6,
ATTRIBUTE7,
ATTRIBUTE8,
ATTRIBUTE9,
ATTRIBUTE10,
SOURCE,
HOLD_ID
)
VALUES
(
TAB_LIST(I).RECORD_ID,
TAB_LIST(I).SYSTEM_NAME,
TAB_LIST(I).TO_ADDRESS,
TAB_LIST(I).EMAIL_SUBJECT,
TAB_LIST(I).CONTENT_BODY,
TAB_LIST(I).PROCESS_NAME,
TAB_LIST(I).PROJECT_NAME,
TAB_LIST(I).TITLE,
TAB_LIST(I).INSTANCE_ID,
ZBR_DEPOT_LOC_LKP(TAB_LIST(I).DEPOT_LOC),
TAB_LIST(I).DIRECTION,
TAB_LIST(I).TRANSACTION_NUMBER,
ZBR_DBLOGGER_TZ_CONV(TAB_LIST(I).CREATION_DATE),
ZBR_DBLOGGER_TZ_CONV(TAB_LIST(I).MODIFY_DATE),
TAB_LIST(I).SR_LINE_NUMBER,
TAB_LIST(I).DEPOT_CODE,
TAB_LIST(I).TRANSACTION_TYPE,
TAB_LIST(I).ERROR_MESSAGE,
TAB_LIST(I).SR_NUMBER,
TAB_LIST(I).STATUS,
TAB_LIST(I).COMP_TITLE,
TAB_LIST(I).CUBE_CIKEY,
TAB_LIST(I).COMP_COMPOSITE_DN,
DBMS_LOB.SUBSTR(TAB_LIST(I).TP_PAYLOAD,3000,1),
DBMS_LOB.SUBSTR(TAB_LIST(I).TP_PAYLOAD,3000,3001),
DBMS_LOB.SUBSTR(TAB_LIST(I).TP_PAYLOAD,3000,6001),
DBMS_LOB.SUBSTR(TAB_LIST(I).TP_PAYLOAD,3000,9001),
DBMS_LOB.SUBSTR(TAB_LIST(I).SIEBEL_PAYLOAD,3000,1),
DBMS_LOB.SUBSTR(TAB_LIST(I).SIEBEL_PAYLOAD,3000,3001),
DBMS_LOB.SUBSTR(TAB_LIST(I).SIEBEL_PAYLOAD,3000,6001),
DBMS_LOB.SUBSTR(TAB_LIST(I).SIEBEL_PAYLOAD,3000,9001),
TAB_LIST(I).SIEBEL_ACK_STR,
TAB_LIST(I).SANMINA_ACK_STR,
TAB_LIST(I).RECORD_INSERTION_DATE,
TAB_LIST(I).RECORD_STATUS,
TAB_LIST(I).AUDIT_PAYLOAD_BLOB,
TAB_LIST(I).ATTRIBUTE1,
TAB_LIST(I).ATTRIBUTE2,
TAB_LIST(I).ATTRIBUTE3,
TAB_LIST(I).ATTRIBUTE4,
TAB_LIST(I).ATTRIBUTE5,
TAB_LIST(I).ATTRIBUTE6,
TAB_LIST(I).ATTRIBUTE7,
TAB_LIST(I).ATTRIBUTE8,
TAB_LIST(I).ATTRIBUTE9,
TAB_LIST(I).ATTRIBUTE10,
TAB_LIST(I).SOURCE,
TAB_LIST(I).HOLD_ID
);

COMMIT;
L_STATUS:='SUCCESS';
L_MSG:=TAB_LIST.COUNT||' RECORDS INSERTED SUCCESSFULLY';
P_STATUS:=L_STATUS;
P_MSG:=L_MSG;

EXCEPTION
WHEN OTHERS THEN 
L_STATUS:='FAILED';
L_MSG:='UNEXPECTED ERROR OCCURED WHILE INSERTING :'||SQLERRM;
P_STATUS:=L_STATUS;
P_MSG:=L_MSG;

END;

PROCEDURE ZBR_DBLOGGER_VB_FETCH(P_IN_VALUE IN ZBR_DBLOGGER_VB_IP_TAB_REC,P_OUT_VALUE OUT ZBR_DBLOGGER_VB_OP_TAB_LIST,P_STATUS OUT VARCHAR2,P_MSG OUT VARCHAR2)
AS
L_STATUS VARCHAR2(20);
L_MSG VARCHAR2(4000);
BEGIN
SELECT ZBR_DBLOGGER_VB_OP_TAB_REC(
ROWID,
RECORD_ID,
SYSTEM_NAME,
TO_ADDRESS,
EMAIL_SUBJECT,
CONTENT_BODY,
PROCESS_NAME,
PROJECT_NAME,
TITLE,
INSTANCE_ID,
REGION,
DIRECTION,
TRANSACTION_NUMBER,
CREATION_DATE,
MODIFY_DATE,
SR_LINE_NUMBER,
DEPOT_CODE,
TRANSACTION_TYPE,
ERROR_MESSAGE,
SR_NUMBER,
STATUS,
COMP_TITLE,
CUBE_CIKEY,
COMP_COMPOSITE_DN,
SANMINA_PAYLOAD_STR_1,
SANMINA_PAYLOAD_STR_2,
SANMINA_PAYLOAD_STR_3,
SANMINA_PAYLOAD_STR_4,
SIEBEL_PAYLOAD_STR_1,
SIEBEL_PAYLOAD_STR_2,
SIEBEL_PAYLOAD_STR_3,
SIEBEL_PAYLOAD_STR_4,
SIEBEL_ACK_STR,
SANMINA_ACK_STR,
RECORD_INSERTION_DATE,
RECORD_STATUS,
AUDIT_PAYLOAD_BLOB,
ATTRIBUTE1,
ATTRIBUTE2,
ATTRIBUTE3,
ATTRIBUTE4,
ATTRIBUTE5,
ATTRIBUTE6,
ATTRIBUTE7,
ATTRIBUTE8,
ATTRIBUTE9,
ATTRIBUTE10,
SOURCE,
HOLD_ID
) bulk collect into P_OUT_VALUE  
 FROM ZEB_TSO_DBLOGGER WHERE LENGTH(CREATION_DATE)>28
 and (1=(CASE WHEN P_IN_VALUE.SR_NUMBER IS NULL THEN 1 ELSE 0 END) or SR_NUMBER=P_IN_VALUE.SR_NUMBER)
 and (1=(CASE WHEN P_IN_VALUE.SR_LINE_NUMBER IS NULL THEN 1 ELSE 0 END) or SR_LINE_NUMBER=P_IN_VALUE.SR_LINE_NUMBER)
 and (1=(CASE WHEN P_IN_VALUE.TRANSACTION_NUMBER IS NULL THEN 1 ELSE 0 END) or TRANSACTION_NUMBER=P_IN_VALUE.TRANSACTION_NUMBER)
 and (1=(CASE WHEN P_IN_VALUE.FROM_DATE IS NULL THEN 1 ELSE 0 END) or ZBR_DBLOGGER_TIMESTAMP(CREATION_DATE)>=P_IN_VALUE.FROM_DATE)
 and (1=(CASE WHEN P_IN_VALUE.TILL_DATE IS NULL THEN 1 ELSE 0 END) or ZBR_DBLOGGER_TIMESTAMP(CREATION_DATE) <=P_IN_VALUE.TILL_DATE);
 
L_STATUS:='SUCCESS';
L_MSG:=SQL%ROWCOUNT||' RECORDS FETCHED SUCCESSFULLY';
P_STATUS:=L_STATUS;
P_MSG:=L_MSG;

EXCEPTION
WHEN OTHERS THEN
L_STATUS:='FAILED';
L_MSG:='UNEXPECTED ERROR OCCURED WHILE FETCHING RECORDS FROM DBLOGGER: '||SQLERRM;
P_STATUS:=L_STATUS;
P_MSG:=L_MSG;
P_OUT_VALUE :=NULL;

END ZBR_DBLOGGER_VB_FETCH;


end ZBR_DBLOGGER_TABLE_LOAD_PKG;