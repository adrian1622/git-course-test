CREATE OR REPLACE PROCEDURE DMESCO.SP_ATRA_CALCULO_PRORRATEO_ESC_NAC (
   EJERCICIO_INDICADOR   IN VARCHAR2,
   FECHA_INFORMACION     IN VARCHAR2,
   TIPO_CORTE            IN VARCHAR2)
IS
   LIMIT_IN                      INTEGER := 100;
   P_CLAVE_EJERCICIO_INDICADOR   VARCHAR2 (8 CHAR) := EJERCICIO_INDICADOR; --'2020ADAS';
   --P_FECHA_INFORMACION           DATE
     -- := TO_DATE (FECHA_INFORMACION, 'YYYY-MM-DD');
   P_CORTE                       VARCHAR2 (4 CHAR) := TIPO_CORTE;     --'OFI';
   V_ETAPA_ATRACCION             VARCHAR2 (18 CHAR); 
   --V_CAMPO_IND                   VARCHAR2 (30 CHAR);
   V_VALOR_IND                   VARCHAR2 (18 CHAR);
   V_SQL_STMT                    VARCHAR2 (32000 CHAR);
   V_SQL_STMT_TOTAL              VARCHAR2 (32000 CHAR);
   V_SQL_STMT_PRE_DISTRIB        VARCHAR2 (32000 CHAR);
   V_SQL_STMT_DISTRIBUCION       VARCHAR2 (32000 CHAR);
   V_SQL_STMT_PCT                VARCHAR2 (32000 CHAR);

   TYPE UNIVERSOCURTYPE IS REF CURSOR;

   C_UNIVERSO                    UNIVERSOCURTYPE;
   C_UNIVERSO_TOTAL              UNIVERSOCURTYPE;
   C_UNIVERSO_DISTRIBUCION       UNIVERSOCURTYPE;
   C_UNIVERSO_PRE_DISTRIB        UNIVERSOCURTYPE;
   C_UNIVERSO_PCT                UNIVERSOCURTYPE;

   TYPE UNIVERSO_RT IS RECORD
   (
      CLAVE_EJERCICIO_INDICADOR    DMESCO.ATRA_CFG_ESCUELA_NAC_PRORRATEO.CLAVE_EJERCICIO_INDICADOR%TYPE,
      CLAVE_CAMPUS                 DMESCO.ATRA_CFG_ESCUELA_NAC_PRORRATEO.CLAVE_CAMPUS%TYPE,
      CLAVE_MAJOR_PGMA_ACAD        DMESCO.ATRA_CFG_ESCUELA_NAC_PRORRATEO.CLAVE_MAJOR_PGMA_ACAD%TYPE,
      ETAPA                        DMESCO.ATRA_CFG_ESCUELA_NAC_PRORRATEO.ETAPA%TYPE,
      TIPO_CARGA                   DMESCO.ATRA_CFG_ESCUELA_NAC_PRORRATEO.TIPO_CARGA%TYPE,
      CLAVE_ESCUELA_NACIONAL       DMESCO.ATRA_CFG_ESCUELA_NAC_PRORRATEO.CLAVE_ESCUELA_NACIONAL%TYPE,
      TOTAL_REGISTROS_DISTRIBUIR   DMESCO.ATRA_CFG_ESCUELA_NAC_PRORRATEO.TOTAL_REGISTROS_DISTRIBUIR%TYPE,
      PORCENTAJE_PRORRATEO         DMESCO.ATRA_CFG_ESCUELA_NAC_PRORRATEO.PORCENTAJE_PRORRATEO%TYPE,
      FECHA_INICIO_PRORRATEO       DMESCO.ATRA_CFG_ESCUELA_NAC_PRORRATEO.FECHA_INICIO_PRORRATEO%TYPE,
      FECHA_FIN_PRORRATEO          DMESCO.ATRA_CFG_ESCUELA_NAC_PRORRATEO.FECHA_FIN_PRORRATEO%TYPE,
      ESTATUS_CARGA                DMESCO.ATRA_CFG_ESCUELA_NAC_PRORRATEO.ESTATUS_CARGA%TYPE,
      ACCION                       VARCHAR2 (3)
   );

   TYPE UNIVERSO_TOTAL_RT IS RECORD
   (
      CLAVE_EJERCICIO_INDICADOR    DMESCO.ATRA_CFG_ESCUELA_NAC_PRORRATEO.CLAVE_EJERCICIO_INDICADOR%TYPE,
      CLAVE_CAMPUS                 DMESCO.ATRA_CFG_ESCUELA_NAC_PRORRATEO.CLAVE_CAMPUS%TYPE,
      CLAVE_MAJOR_PGMA_ACAD        DMESCO.ATRA_CFG_ESCUELA_NAC_PRORRATEO.CLAVE_MAJOR_PGMA_ACAD%TYPE,
      ETAPA                        DMESCO.ATRA_CFG_ESCUELA_NAC_PRORRATEO.ETAPA%TYPE,
      TOTAL_REGISTROS_DISTRIBUIR   DMESCO.ATRA_CFG_ESCUELA_NAC_PRORRATEO.TOTAL_REGISTROS_DISTRIBUIR%TYPE,
      ACCION                       VARCHAR2 (3)
   );

   TYPE UNIVERSO_PCT_RT IS RECORD
   (
      CLAVE_EJERCICIO_INDICADOR   DMESCO.ATRA_CFG_ESCUELA_NAC_PRORRATEO.CLAVE_EJERCICIO_INDICADOR%TYPE,
      CLAVE_CAMPUS                DMESCO.ATRA_CFG_ESCUELA_NAC_PRORRATEO.CLAVE_CAMPUS%TYPE,
      CLAVE_MAJOR_PGMA_ACAD       DMESCO.ATRA_CFG_ESCUELA_NAC_PRORRATEO.CLAVE_MAJOR_PGMA_ACAD%TYPE,
      ETAPA                       DMESCO.ATRA_CFG_ESCUELA_NAC_PRORRATEO.ETAPA%TYPE,
      CLAVE_ESCUELA_NACIONAL      DMESCO.ATRA_CFG_ESCUELA_NAC_PRORRATEO.CLAVE_ESCUELA_NACIONAL%TYPE,
      PORCENTAJE_PRORRATEO        DMESCO.ATRA_CFG_ESCUELA_NAC_PRORRATEO.PORCENTAJE_PRORRATEO%TYPE,
      ACCION                      VARCHAR2 (3)
   );

   TYPE UNIVERSO_DISTRIBUCION_RT IS RECORD
   (
      ROW_ID                        ROWID,
      CLAVE_ESCUELA_NAC_PRORRATEO   DMESCO.ATRA_DET_SOLICITANTE.CLAVE_ESCUELA_NAC_PRORRATEO%TYPE,
      ACCION                        VARCHAR2 (3)
   );

   TYPE UNIVERSO_PRE_DISTRIB_RT IS RECORD
   (
      ROW_ID                   ROWID,
      CLAVE_ESCUELA_NACIONAL   DMESCO.ATRA_DET_SOLICITANTE.CLAVE_ESCUELA_NACIONAL%TYPE,
      ACCION                   VARCHAR2 (3)
   );

   TYPE UNIVERSO_AAT IS TABLE OF UNIVERSO_RT
      INDEX BY PLS_INTEGER;

   TYPE UNIVERSO_TOTAL_AAT IS TABLE OF UNIVERSO_TOTAL_RT
      INDEX BY PLS_INTEGER;

   TYPE UNIVERSO_PRE_DISTRIB_AAT IS TABLE OF UNIVERSO_PRE_DISTRIB_RT
      INDEX BY PLS_INTEGER;

   TYPE UNIVERSO_DISTRIBUCION_AAT IS TABLE OF UNIVERSO_DISTRIBUCION_RT
      INDEX BY PLS_INTEGER;

   TYPE UNIVERSO_PCT_AAT IS TABLE OF UNIVERSO_PCT_RT
      INDEX BY PLS_INTEGER;

   ROWS_UNIVERSO                 UNIVERSO_AAT;
   ROWS_UPDATE                   UNIVERSO_AAT;
   ROWS_INSERT                   UNIVERSO_AAT;

   ROWS_UNIVERSO_TOTAL           UNIVERSO_TOTAL_AAT;
   ROWS_UPDATE_TOTAL             UNIVERSO_TOTAL_AAT;

   ROWS_UNIVERSO_DISTRIBUCION    UNIVERSO_DISTRIBUCION_AAT;
   ROWS_UPDATE_DISTRIBUCION      UNIVERSO_DISTRIBUCION_AAT;

   ROWS_UNIVERSO_PRE_DISTRIB     UNIVERSO_PRE_DISTRIB_AAT;
   ROWS_UPDATE_PRE_DISTRIB       UNIVERSO_PRE_DISTRIB_AAT;

   ROWS_UNIVERSO_PCT             UNIVERSO_PCT_AAT;
   ROWS_UPDATE_PCT               UNIVERSO_PCT_AAT;
BEGIN
   --recupera la etapa atraccion se usa MAX por si en dado caso hay dos cortes o regresa mas de un registro
   SELECT MAX (ETAPA_ATRACCION)
     INTO V_ETAPA_ATRACCION
     FROM DMESCO.IND_FECHA_EQUIVALENTE
    WHERE     CLAVE_EJER_IND_ACTUAL = P_CLAVE_EJERCICIO_INDICADOR
          AND CORTE_IND_ACTUAL = P_CORTE
          ;        
    
    
   IF V_ETAPA_ATRACCION = 'Inscritos'
   THEN
      --V_CAMPO_IND := 'IND_INSCRITO';
      V_VALOR_IND := 'IND_INSCRITO';
   ELSIF V_ETAPA_ATRACCION = 'Ipps + Inscritos' then
      --V_CAMPO_IND := 'IND_IPP_INSCRITO';
      V_VALOR_IND := 'IND_IPP_INSCRITO'; 
   ELSIF V_ETAPA_ATRACCION = 'Ipps'
   THEN
      --V_CAMPO_IND := 'IND_IPP_OFICIAL';
      V_VALOR_IND := 'IND_IPP_OFICIAL';
   END IF;

   --universo de registros en automatico a insertar o actualizar
   V_SQL_STMT :=
         '
        SELECT A.CLAVE_EJERCICIO_INDICADOR,
               A.CLAVE_CAMPUS,
               A.CLAVE_MAJOR_PGMA_ACAD,'''
      || V_VALOR_IND
      || '''
                AS ETAPA,
               ''A'' AS TIPO_CARGA,
               C.CLAVE_ESCUELA_NACIONAL,               
               NULL as PORCENTAJE_PRORRATEO,
               NULL as TOTAL_REGISTROS_DISTRIBUIR,
               trunc(SYSDATE) AS FECHA_INICIO_PRORRATEO,
               trunc(SYSDATE) AS FECHA_FIN_PRORRATEO,
               ''A'' AS ESTATUS_CARGA,
                CASE
                   WHEN NVL (B.CLAVE_EJERCICIO_INDICADOR, ''INS'') != ''INS''
                   THEN
                      ''UPD''
                   ELSE
                      ''INS''
                END
                   AS ACCION
          FROM DMESCO.ATRA_DET_SOLICITANTE A
               JOIN DMESCO.ATRA_REL_MAJOR_ESCUELA C
               ON A.CLAVE_MAJOR_SOLICITANTE = C.CLAVE_MAJOR_PGMA_ACAD
               LEFT JOIN
               DMESCO.ATRA_CFG_ESCUELA_NAC_PRORRATEO B
                  ON     A.CLAVE_EJERCICIO_INDICADOR =
                            B.CLAVE_EJERCICIO_INDICADOR
                     AND A.CLAVE_CAMPUS = B.CLAVE_CAMPUS
                     AND A.CLAVE_MAJOR_PGMA_ACAD = B.CLAVE_MAJOR_PGMA_ACAD
                     AND C.CLAVE_ESCUELA_NACIONAL = B.CLAVE_ESCUELA_NACIONAL
                     AND B.TIPO_CARGA = ''A''
                     AND B.ETAPA =  '''
      || V_VALOR_IND
      || '''                     
         WHERE     A.CLAVE_EJERCICIO_INDICADOR = '''
      || P_CLAVE_EJERCICIO_INDICADOR
      || '''
               AND A.CORTE_ATRA_DET_SOLICITANTE = '''
      || P_CORTE
      || '''
               AND A.FECHA_INFORMACION = TO_DATE ( '''||FECHA_INFORMACION ||''', ''YYYY-MM-DD'')               
               AND '
      || V_VALOR_IND
      || ' = ''SI''                           
      AND C.CLAVE_ESCUELA_NACIONAL not in (10,11)      
      AND A.CLAVE_MAJOR_PGMA_ACAD IN (''ESC'', ''AMC'')
      AND IND_CONSIDERADO_BASE = ''SI''
      GROUP BY A.CLAVE_EJERCICIO_INDICADOR,
               A.CLAVE_CAMPUS,
               A.CLAVE_MAJOR_PGMA_ACAD,
               B.CLAVE_EJERCICIO_INDICADOR,
               C.CLAVE_ESCUELA_NACIONAL';

   --DBMS_OUTPUT.PUT_LINE(V_SQL_STMT);
   --actualiza el estatus de carga a I Inactivo antes de procesar la informacion
   -- para tipo carga A Automatico
   EXECUTE IMMEDIATE 'UPDATE DMESCO.ATRA_CFG_ESCUELA_NAC_PRORRATEO
                      SET ESTATUS_CARGA= ''I'' 
                      WHERE TIPO_CARGA = ''A''';

   OPEN C_UNIVERSO FOR V_SQL_STMT;

   LOOP
      FETCH C_UNIVERSO
         BULK COLLECT INTO ROWS_UNIVERSO
         LIMIT LIMIT_IN;

      --EXIT WHEN C_UNIVERSO_IND_INSCRITO%NOTFOUND;
      FOR I IN 1 .. ROWS_UNIVERSO.COUNT
      LOOP
         IF ROWS_UNIVERSO (I).ACCION = 'UPD'
         THEN
            ROWS_UPDATE (I) := ROWS_UNIVERSO (I);
         ELSIF ROWS_UNIVERSO (I).ACCION = 'INS'
         THEN
            ROWS_INSERT (I) := ROWS_UNIVERSO (I);
         END IF;
      END LOOP;


      FORALL INDX IN INDICES OF ROWS_INSERT
         INSERT
           INTO DMESCO.ATRA_CFG_ESCUELA_NAC_PRORRATEO (
                   CLAVE_EJERCICIO_INDICADOR,
                   CLAVE_CAMPUS,
                   CLAVE_MAJOR_PGMA_ACAD,
                   ETAPA,
                   TIPO_CARGA,
                   CLAVE_ESCUELA_NACIONAL,
                   PORCENTAJE_PRORRATEO,
                   FECHA_INICIO_PRORRATEO,
                   FECHA_FIN_PRORRATEO,
                   ESTATUS_CARGA,
                   FECHA_ULTIMA_MODIFICACION)
         VALUES (ROWS_INSERT (INDX).CLAVE_EJERCICIO_INDICADOR,
                 ROWS_INSERT (INDX).CLAVE_CAMPUS,
                 ROWS_INSERT (INDX).CLAVE_MAJOR_PGMA_ACAD,
                 ROWS_INSERT (INDX).ETAPA,
                 ROWS_INSERT (INDX).TIPO_CARGA,
                 ROWS_INSERT (INDX).CLAVE_ESCUELA_NACIONAL,
                 ROWS_INSERT (INDX).PORCENTAJE_PRORRATEO,
                 ROWS_INSERT (INDX).FECHA_INICIO_PRORRATEO,
                 ROWS_INSERT (INDX).FECHA_FIN_PRORRATEO,
                 ROWS_INSERT (INDX).ESTATUS_CARGA,
                 SYSDATE);

      FORALL INDX IN INDICES OF ROWS_UPDATE
         UPDATE DMESCO.ATRA_CFG_ESCUELA_NAC_PRORRATEO
            SET PORCENTAJE_PRORRATEO = ROWS_UPDATE (INDX).PORCENTAJE_PRORRATEO,
                FECHA_INICIO_PRORRATEO =
                   ROWS_UPDATE (INDX).FECHA_INICIO_PRORRATEO,
                FECHA_FIN_PRORRATEO = ROWS_UPDATE (INDX).FECHA_FIN_PRORRATEO,
                ESTATUS_CARGA = ROWS_UPDATE (INDX).ESTATUS_CARGA,
                FECHA_ULTIMA_MODIFICACION = SYSDATE
          WHERE     CLAVE_EJERCICIO_INDICADOR =
                       ROWS_UPDATE (INDX).CLAVE_EJERCICIO_INDICADOR
                AND CLAVE_CAMPUS = ROWS_UPDATE (INDX).CLAVE_CAMPUS
                AND CLAVE_MAJOR_PGMA_ACAD =
                       ROWS_UPDATE (INDX).CLAVE_MAJOR_PGMA_ACAD
                AND ETAPA = ROWS_UPDATE (INDX).ETAPA
                AND TIPO_CARGA = ROWS_UPDATE (INDX).TIPO_CARGA
                AND CLAVE_ESCUELA_NACIONAL =
                       ROWS_UPDATE (INDX).CLAVE_ESCUELA_NACIONAL;

      ROWS_INSERT.DELETE;
      ROWS_UPDATE.DELETE;
      COMMIT;
      EXIT WHEN ROWS_UNIVERSO.COUNT < LIMIT_IN;
   END LOOP;

   CLOSE C_UNIVERSO;

   --2da parte actualiza el porcentaje en automatico
   V_SQL_STMT_PCT :=
         ' SELECT A.CLAVE_EJERCICIO_INDICADOR,
             A.CLAVE_CAMPUS,
             A.CLAVE_MAJOR_PGMA_ACAD,
             A.ETAPA,
             A.CLAVE_ESCUELA_NACIONAL,
             --A.DESC_ESCUELA,
             --A.TOTAL_CLAVE_MAJOR_AD_HE_IC,
             --A.TOTAL_CLAVE_MAJOR_CAMPUS,             
             ROUND (TOTAL_CLAVE_MAJOR_AD_HE_IC / TOTAL_CLAVE_MAJOR_CAMPUS, 3)
                AS PORCENTAJE_PRORRATEO,
                ''UPD'' as ACCION                                            
        FROM (
        SELECT DISTINCT
                     A.CLAVE_EJERCICIO_INDICADOR,
                     A.CLAVE_CAMPUS,
                     A.CLAVE_MAJOR_PGMA_ACAD,
                     ETAPA,
                     D.CLAVE_ESCUELA_NACIONAL,
                     B.DESC_ESCUELA,
                     COUNT (
                        0)
                     OVER (
                        PARTITION BY A.CLAVE_EJERCICIO_INDICADOR,
                                     A.CLAVE_CAMPUS,
                                     A.CLAVE_MAJOR_PGMA_ACAD,
                                     D.CLAVE_ESCUELA_NACIONAL
                                     )
                        AS TOTAL_CLAVE_MAJOR_AD_HE_IC,
                     COUNT (
                        0)
                     OVER (
                        PARTITION BY A.CLAVE_EJERCICIO_INDICADOR,
                                     A.CLAVE_CAMPUS,
                                     A.CLAVE_MAJOR_PGMA_ACAD)
                        AS TOTAL_CLAVE_MAJOR_CAMPUS
                FROM DMESCO.ATRA_DET_SOLICITANTE A
                     JOIN DMESCO.ATRA_REL_MAJOR_ESCUELA D
                    ON A.CLAVE_MAJOR_SOLICITANTE = D.CLAVE_MAJOR_PGMA_ACAD    
                    JOIN
                     DMESCO.ATRA_DIM_ESCUELA_NACIONAL B
                        ON D.CLAVE_ESCUELA_NACIONAL =
                              B.CLAVE_ESCUELA_NACIONAL
                     JOIN
                     DMESCO.ATRA_CFG_ESCUELA_NAC_PRORRATEO C
                        ON     A.CLAVE_EJERCICIO_INDICADOR =
                                  C.CLAVE_EJERCICIO_INDICADOR
                           AND A.CLAVE_CAMPUS = C.CLAVE_CAMPUS
                           AND A.CLAVE_MAJOR_PGMA_ACAD =
                                  C.CLAVE_MAJOR_PGMA_ACAD
                           AND D.CLAVE_ESCUELA_NACIONAL =
                                  C.CLAVE_ESCUELA_NACIONAL
                           AND ESTATUS_CARGA = ''A''
                           AND TIPO_CARGA = ''A''
               WHERE     A.CLAVE_EJERCICIO_INDICADOR = '''
      || P_CLAVE_EJERCICIO_INDICADOR
      || '''
                     AND A.CORTE_ATRA_DET_SOLICITANTE = '''
      || P_CORTE
      || '''
               AND A.FECHA_INFORMACION = TO_DATE ( '''||FECHA_INFORMACION ||''', ''YYYY-MM-DD'')               
                     AND D.CLAVE_ESCUELA_NACIONAL NOT IN (10, 11)
                     AND A.CLAVE_MAJOR_PGMA_ACAD IN (''ESC'', ''AMC'')
                     AND A.IND_CONSIDERADO_BASE =''SI''
                     AND '
      || V_VALOR_IND
      || ' = ''SI'' ) A';

   --DBMS_OUTPUT.PUT_LINE(V_SQL_STMT_PCT);

   OPEN C_UNIVERSO_PCT FOR V_SQL_STMT_PCT;

   LOOP
      FETCH C_UNIVERSO_PCT
         BULK COLLECT INTO ROWS_UNIVERSO_PCT
         LIMIT LIMIT_IN;

      --EXIT WHEN C_UNIVERSO_IND_INSCRITO%NOTFOUND;
      FOR I IN 1 .. ROWS_UNIVERSO_PCT.COUNT
      LOOP
         IF ROWS_UNIVERSO_PCT (I).ACCION = 'UPD'
         THEN
            ROWS_UPDATE_PCT (I) := ROWS_UNIVERSO_PCT (I);
         END IF;
      END LOOP;

      FORALL INDX IN INDICES OF ROWS_UPDATE_PCT
         UPDATE DMESCO.ATRA_CFG_ESCUELA_NAC_PRORRATEO
            SET PORCENTAJE_PRORRATEO =
                   ROWS_UPDATE_PCT (INDX).PORCENTAJE_PRORRATEO,
                FECHA_ULTIMA_MODIFICACION = SYSDATE
          WHERE     CLAVE_EJERCICIO_INDICADOR =
                       ROWS_UPDATE_PCT (INDX).CLAVE_EJERCICIO_INDICADOR
                AND CLAVE_CAMPUS = ROWS_UPDATE_PCT (INDX).CLAVE_CAMPUS
                AND CLAVE_MAJOR_PGMA_ACAD =
                       ROWS_UPDATE_PCT (INDX).CLAVE_MAJOR_PGMA_ACAD
                AND ETAPA = ROWS_UPDATE_PCT (INDX).ETAPA
                AND CLAVE_ESCUELA_NACIONAL =
                       ROWS_UPDATE_PCT (INDX).CLAVE_ESCUELA_NACIONAL;

      ROWS_UPDATE_PCT.DELETE;
      COMMIT;
      EXIT WHEN ROWS_UNIVERSO_PCT.COUNT < LIMIT_IN;
   END LOOP;

   CLOSE C_UNIVERSO_PCT;

   ---
   --3r parte actualizar tabla prorrateo el campo total
   V_SQL_STMT_TOTAL :=
         '
SELECT CLAVE_EJERCICIO_INDICADOR,
         CLAVE_CAMPUS,
         CLAVE_MAJOR_PGMA_ACAD,'''
      || V_VALOR_IND
      || '''
                AS ETAPA,       
       COUNT (0) AS TOTAL_REGISTROS_DISTRIBUIR,
       ''UPD'' as ACCION  
    FROM DMESCO.ATRA_DET_SOLICITANTE
   WHERE CLAVE_EJERCICIO_INDICADOR = '''
      || P_CLAVE_EJERCICIO_INDICADOR
      || '''
               AND FECHA_INFORMACION = TO_DATE ( '''||FECHA_INFORMACION ||''', ''YYYY-MM-DD'')
               AND CORTE_ATRA_DET_SOLICITANTE = '''
      || P_CORTE
      || '''                        
      AND CLAVE_ESCUELA_NACIONAL in (10,11)      
      AND CLAVE_MAJOR_PGMA_ACAD IN (''ESC'', ''AMC'')
      AND IND_CONSIDERADO_BASE = ''SI''         
GROUP BY CLAVE_EJERCICIO_INDICADOR, CLAVE_CAMPUS, CLAVE_MAJOR_PGMA_ACAD
';

   --DBMS_OUTPUT.PUT_LINE(V_SQL_STMT_TOTAL);

   OPEN C_UNIVERSO_TOTAL FOR V_SQL_STMT_TOTAL;

   LOOP
      FETCH C_UNIVERSO_TOTAL
         BULK COLLECT INTO ROWS_UNIVERSO_TOTAL
         LIMIT LIMIT_IN;

      --EXIT WHEN C_UNIVERSO_IND_INSCRITO%NOTFOUND;
      FOR I IN 1 .. ROWS_UNIVERSO_TOTAL.COUNT
      LOOP
         IF ROWS_UNIVERSO_TOTAL (I).ACCION = 'UPD'
         THEN
            ROWS_UPDATE_TOTAL (I) := ROWS_UNIVERSO_TOTAL (I);
         END IF;
      END LOOP;

      FORALL INDX IN INDICES OF ROWS_UPDATE_TOTAL
         UPDATE DMESCO.ATRA_CFG_ESCUELA_NAC_PRORRATEO
            SET TOTAL_REGISTROS_DISTRIBUIR =
                   ROWS_UPDATE_TOTAL (INDX).TOTAL_REGISTROS_DISTRIBUIR,
                FECHA_ULTIMA_MODIFICACION = SYSDATE
          WHERE     CLAVE_EJERCICIO_INDICADOR =
                       ROWS_UPDATE_TOTAL (INDX).CLAVE_EJERCICIO_INDICADOR
                AND CLAVE_CAMPUS = ROWS_UPDATE_TOTAL (INDX).CLAVE_CAMPUS
                AND CLAVE_MAJOR_PGMA_ACAD =
                       ROWS_UPDATE_TOTAL (INDX).CLAVE_MAJOR_PGMA_ACAD
                --AND ETAPA = ROWS_UPDATE_TOTAL (INDX).ETAPA 
                AND ESTATUS_CARGA = 'A'
                                                          ;

      ROWS_UPDATE_TOTAL.DELETE;
      COMMIT;
      EXIT WHEN ROWS_UNIVERSO_TOTAL.COUNT < LIMIT_IN;
   END LOOP;

   CLOSE C_UNIVERSO_TOTAL;

   V_SQL_STMT_PRE_DISTRIB :=
         '
   SELECT rowid as row_id, 
         CLAVE_ESCUELA_NACIONAL ,
         ''UPD'' AS ACCION
  FROM DMESCO.ATRA_DET_SOLICITANTE
 WHERE     CLAVE_EJERCICIO_INDICADOR = '''
      || P_CLAVE_EJERCICIO_INDICADOR
      || '''
                       AND CORTE_ATRA_DET_SOLICITANTE = '''
      || P_CORTE
      || '''
               AND FECHA_INFORMACION = TO_DATE ( '''||FECHA_INFORMACION ||''', ''YYYY-MM-DD'')
      AND IND_CONSIDERADO_BASE = ''SI''  
   ';

   --dbms_output.put_line (V_SQL_STMT_PRE_DISTRIB);
   -- update table pre distribucion
   OPEN C_UNIVERSO_PRE_DISTRIB FOR V_SQL_STMT_PRE_DISTRIB;

   LOOP
      FETCH C_UNIVERSO_PRE_DISTRIB
         BULK COLLECT INTO ROWS_UNIVERSO_PRE_DISTRIB
         LIMIT LIMIT_IN;

      FOR I IN 1 .. ROWS_UNIVERSO_PRE_DISTRIB.COUNT
      LOOP
         IF ROWS_UNIVERSO_PRE_DISTRIB (I).ACCION = 'UPD'
         THEN
            ROWS_UPDATE_PRE_DISTRIB (I) := ROWS_UNIVERSO_PRE_DISTRIB (I);
         END IF;
      END LOOP;

      FORALL INDX IN INDICES OF ROWS_UPDATE_PRE_DISTRIB
         UPDATE DMESCO.ATRA_DET_SOLICITANTE
            SET CLAVE_ESCUELA_NAC_PRORRATEO =
                   ROWS_UPDATE_PRE_DISTRIB (INDX).CLAVE_ESCUELA_NACIONAL,
                FECHA_ULTIMA_MODIFICACION = SYSDATE
          WHERE ROWID = ROWS_UPDATE_PRE_DISTRIB (INDX).ROW_ID;

      ROWS_UPDATE_PRE_DISTRIB.DELETE;
      COMMIT;
      EXIT WHEN ROWS_UNIVERSO_PRE_DISTRIB.COUNT < LIMIT_IN;
   END LOOP;

   CLOSE C_UNIVERSO_PRE_DISTRIB;


   V_SQL_STMT_DISTRIBUCION :=
         'SELECT ROW_ID,
       --CLAVE_EJERCICIO_INDICADOR,
       --CLAVE_CAMPUS,
       --CLAVE_MAJOR_PGMA_ACAD,
       --ETAPA,
       --IND_ALUMNO_TEC,
       --TOTAL_REGISTROS_DISTRIBUIR,
       --TOTAL_CLAVE_1,
       --TOTAL_CLAVE_2,
       --TOTAL_CLAVE_4,
       --RANK_POSICION,
       --CVE_ESC_NAC_CALC,       
       CASE
          WHEN     CLAVE_MAJOR_PGMA_ACAD = ''AMC''
               AND RANK_POSICION > TOTAL_CLAVE_1 * 2
          THEN
             2
          WHEN     CLAVE_MAJOR_PGMA_ACAD = ''AMC''
               AND RANK_POSICION > TOTAL_CLAVE_2 * 2
          THEN
             1
          WHEN     CLAVE_MAJOR_PGMA_ACAD = ''ESC''
               AND RANK_POSICION > TOTAL_CLAVE_4 * 2
          THEN
             1
          WHEN     CLAVE_MAJOR_PGMA_ACAD = ''ESC''
               AND RANK_POSICION > TOTAL_CLAVE_1 * 2
          THEN
             4
          ELSE
             CVE_ESC_NAC_CALC
       END
          AS CLAVE_ESCUELA_NAC_PRORRATEO,
          ''UPD'' as ACCION
  FROM (SELECT ROW_ID,
               CLAVE_EJERCICIO_INDICADOR,
               CLAVE_CAMPUS,
               CLAVE_MAJOR_PGMA_ACAD,
               ETAPA,
               IND_ALUMNO_TEC,
               TOTAL_REGISTROS_DISTRIBUIR,
               TOTAL_CLAVE_1,
               TOTAL_CLAVE_2,
               TOTAL_CLAVE_4,
               RANK_POSICION,
               CASE
                  WHEN     CLAVE_MAJOR_PGMA_ACAD = ''AMC''
                       AND TOTAL_CLAVE_1 IS NULL
                  THEN
                     2
                  WHEN     CLAVE_MAJOR_PGMA_ACAD = ''AMC''
                       AND TOTAL_CLAVE_2 IS NULL
                  THEN
                     1
                  WHEN     CLAVE_MAJOR_PGMA_ACAD = ''ESC''
                       AND TOTAL_CLAVE_1 IS NULL
                  THEN
                     4
                  WHEN     CLAVE_MAJOR_PGMA_ACAD = ''ESC''
                       AND TOTAL_CLAVE_4 IS NULL
                  THEN
                     1
                  WHEN     CLAVE_MAJOR_PGMA_ACAD = ''AMC''
                       AND TOTAL_CLAVE_1 >= TOTAL_CLAVE_2
                       AND MOD (RANK_POSICION, 2) = 0
                  THEN
                     1
                  WHEN     CLAVE_MAJOR_PGMA_ACAD = ''AMC''
                       AND TOTAL_CLAVE_1 >= TOTAL_CLAVE_2
                       AND MOD (RANK_POSICION, 2) = 1
                  THEN
                     2
                  WHEN     CLAVE_MAJOR_PGMA_ACAD = ''AMC''
                       AND TOTAL_CLAVE_2 >= TOTAL_CLAVE_1
                       AND MOD (RANK_POSICION, 2) = 0
                  THEN
                     2
                  WHEN     CLAVE_MAJOR_PGMA_ACAD = ''AMC''
                       AND TOTAL_CLAVE_2 >= TOTAL_CLAVE_1
                       AND MOD (RANK_POSICION, 2) = 1
                  THEN
                     1
                  WHEN     CLAVE_MAJOR_PGMA_ACAD = ''ESC''
                       AND TOTAL_CLAVE_1 >= TOTAL_CLAVE_4
                       AND MOD (RANK_POSICION, 2) = 0
                  THEN
                     1
                  WHEN     CLAVE_MAJOR_PGMA_ACAD = ''ESC''
                       AND TOTAL_CLAVE_1 >= TOTAL_CLAVE_4
                       AND MOD (RANK_POSICION, 2) = 1
                  THEN
                     4
                  WHEN     CLAVE_MAJOR_PGMA_ACAD = ''ESC''
                       AND TOTAL_CLAVE_4 >= TOTAL_CLAVE_1
                       AND MOD (RANK_POSICION, 2) = 0
                  THEN
                     4
                  WHEN     CLAVE_MAJOR_PGMA_ACAD = ''ESC''
                       AND TOTAL_CLAVE_4 >= TOTAL_CLAVE_1
                       AND MOD (RANK_POSICION, 2) = 1
                  THEN
                     1
               END
                  AS CVE_ESC_NAC_CALC
          FROM (SELECT E.ROWID AS ROW_ID,
                       E.CLAVE_EJERCICIO_INDICADOR,
                       E.CLAVE_CAMPUS,
                       E.CLAVE_MAJOR_PGMA_ACAD,
                       ETAPA,
                       IND_ALUMNO_TEC,
                       TOTAL_REGISTROS_DISTRIBUIR,
                       TOTAL_CLAVE_1,
                       TOTAL_CLAVE_2,
                       TOTAL_CLAVE_4,
                       ROW_NUMBER ()
                       OVER (
                          PARTITION BY A.CLAVE_EJERCICIO_INDICADOR,
                                       A.CLAVE_CAMPUS,
                                       A.CLAVE_MAJOR_PGMA_ACAD
                          ORDER BY A.CLAVE_CAMPUS, IND_ALUMNO_TEC)
                          AS RANK_POSICION
                  FROM (SELECT A.CLAVE_EJERCICIO_INDICADOR,
       A.CLAVE_CAMPUS,
       A.CLAVE_MAJOR_PGMA_ACAD,
       A.ETAPA,
       A.CLAVE_ESCUELA_NACIONAL,
       A.TOTAL_REGISTROS_DISTRIBUIR,
       ROUND (A.PORCENTAJE_PRORRATEO * TOTAL_REGISTROS_DISTRIBUIR, 0)
          AS TOTAL_A_DISTRIBUIR
  FROM DMESCO.ATRA_CFG_ESCUELA_NAC_PRORRATEO A
       JOIN
       (  SELECT CLAVE_EJERCICIO_INDICADOR,
                 CLAVE_CAMPUS,
                 CLAVE_MAJOR_PGMA_ACAD,
                 ETAPA,
                 MAX (TIPO_CARGA) AS MAX_TIPO_CARGA,
                 CLAVE_ESCUELA_NACIONAL
            FROM (SELECT CLAVE_EJERCICIO_INDICADOR,
                         CLAVE_CAMPUS,
                         CLAVE_MAJOR_PGMA_ACAD,
                         ETAPA,
                         TIPO_CARGA,
                         CLAVE_ESCUELA_NACIONAL,
                         PORCENTAJE_PRORRATEO,
                         TOTAL_REGISTROS_DISTRIBUIR,
                         FECHA_INICIO_PRORRATEO,
                         FECHA_FIN_PRORRATEO
                    FROM DMESCO.ATRA_CFG_ESCUELA_NAC_PRORRATEO
                   WHERE ESTATUS_CARGA = ''A'' AND TIPO_CARGA = ''A''
                  UNION
                  SELECT CLAVE_EJERCICIO_INDICADOR,
                         CLAVE_CAMPUS,
                         CLAVE_MAJOR_PGMA_ACAD,
                         ETAPA,
                         TIPO_CARGA,
                         CLAVE_ESCUELA_NACIONAL,
                         PORCENTAJE_PRORRATEO,
                         TOTAL_REGISTROS_DISTRIBUIR,
                         FECHA_INICIO_PRORRATEO,
                         FECHA_FIN_PRORRATEO
                    FROM DMESCO.ATRA_CFG_ESCUELA_NAC_PRORRATEO
                   WHERE     TIPO_CARGA = ''M''
                         AND TRUNC (SYSDATE) BETWEEN FECHA_INICIO_PRORRATEO
                                                 AND FECHA_FIN_PRORRATEO
                         AND ESTATUS_CARGA = ''A'') B
        GROUP BY CLAVE_EJERCICIO_INDICADOR,
                 CLAVE_CAMPUS,
                 CLAVE_MAJOR_PGMA_ACAD,
                 ETAPA,
                 CLAVE_ESCUELA_NACIONAL) B
          ON     A.CLAVE_EJERCICIO_INDICADOR = B.CLAVE_EJERCICIO_INDICADOR
             AND A.CLAVE_CAMPUS = B.CLAVE_CAMPUS
             AND A.CLAVE_MAJOR_PGMA_ACAD = B.CLAVE_MAJOR_PGMA_ACAD
             AND A.ETAPA = B.ETAPA
             AND A.CLAVE_ESCUELA_NACIONAL = B.CLAVE_ESCUELA_NACIONAL
             AND A.TIPO_CARGA = B.MAX_TIPO_CARGA) PIVOT (MIN (
                                                                          TOTAL_A_DISTRIBUIR)
                                                                FOR CLAVE_ESCUELA_NACIONAL
                                                                IN  (''1'' AS TOTAL_CLAVE_1,
                                                                    ''2'' AS TOTAL_CLAVE_2,
                                                                    ''4'' AS TOTAL_CLAVE_4)) A
                       JOIN
                       DMESCO.ATRA_DET_SOLICITANTE E
                          ON     A.CLAVE_EJERCICIO_INDICADOR =
                                    E.CLAVE_EJERCICIO_INDICADOR
                             AND A.CLAVE_CAMPUS = E.CLAVE_CAMPUS
                             AND A.CLAVE_MAJOR_PGMA_ACAD =
                                    E.CLAVE_MAJOR_PGMA_ACAD
                 WHERE     E.CLAVE_EJERCICIO_INDICADOR = '''
      || P_CLAVE_EJERCICIO_INDICADOR
      || '''
                       AND E.CORTE_ATRA_DET_SOLICITANTE = '''
      || P_CORTE
      || '''
               AND E.FECHA_INFORMACION = TO_DATE ( '''||FECHA_INFORMACION ||''', ''YYYY-MM-DD'')      
                 AND E.CLAVE_ESCUELA_NACIONAL IN (10, 11)
                       AND A.CLAVE_MAJOR_PGMA_ACAD IN (''ESC'', ''AMC'')      
      AND IND_CONSIDERADO_BASE = ''SI''           ))
';

   --DBMS_OUTPUT.PUT_LINE (V_SQL_STMT_DISTRIBUCION);

   OPEN C_UNIVERSO_DISTRIBUCION FOR V_SQL_STMT_DISTRIBUCION;

   LOOP
      FETCH C_UNIVERSO_DISTRIBUCION
         BULK COLLECT INTO ROWS_UNIVERSO_DISTRIBUCION
         LIMIT LIMIT_IN;

      FOR I IN 1 .. ROWS_UNIVERSO_DISTRIBUCION.COUNT
      LOOP
         IF ROWS_UNIVERSO_DISTRIBUCION (I).ACCION = 'UPD'
         THEN
            ROWS_UPDATE_DISTRIBUCION (I) := ROWS_UNIVERSO_DISTRIBUCION (I);
         END IF;
      END LOOP;

      FORALL INDX IN INDICES OF ROWS_UPDATE_DISTRIBUCION
         UPDATE DMESCO.ATRA_DET_SOLICITANTE
            SET CLAVE_ESCUELA_NAC_PRORRATEO =
                   ROWS_UPDATE_DISTRIBUCION (INDX).CLAVE_ESCUELA_NAC_PRORRATEO,
                FECHA_ULTIMA_MODIFICACION = SYSDATE
          WHERE ROWID = ROWS_UPDATE_DISTRIBUCION (INDX).ROW_ID;

      ROWS_UPDATE_DISTRIBUCION.DELETE;
      COMMIT;
      EXIT WHEN ROWS_UNIVERSO_DISTRIBUCION.COUNT < LIMIT_IN;
   END LOOP;

   CLOSE C_UNIVERSO_DISTRIBUCION;
END;
/