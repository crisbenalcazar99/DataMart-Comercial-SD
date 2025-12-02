SELECT 
    cedula, 
	 anos AS vigencia, 
	 fecha_aprob AS fecha_aprobacion, 
	 fecha_emision,
	 'FIRMA ELECTRONICA' AS  producto,
	 case when estado_pn = 1 then 'Aprobado'
	 when estado_pn = 5 then 'Emitida'
	 END AS estado_firma,
	 medio AS mediocam, 
    null AS razon_social, 
	 ruc_pn AS ruc, 
	 'PN' AS tipo_firma,
	 SERIAL AS serial_firma, 
    fecha_caduca_v2 AS "fecha_caducidad",
    correo, 
	 telefono, 
	 CONCAT(nombre," ", ap1, " ", ap2) AS nombre
FROM Certificados_Electronicos_Subca1.persona_natural
WHERE estado_pn IN (1, 5) 
  AND nombre NOT LIKE '%prueb%'
  AND ap1 NOT LIKE '%prueb%'
  AND ap2 NOT LIKE '%prueb%'
  AND nombre != 'OPERADOR SD'
  AND nombre != 'OPERADOR TELCONET LOJA'
  AND cedula != '1313628800'
  AND nombre NOT LIKE '%test%'
  AND ap1 NOT LIKE '%test%'
  AND ap2 NOT LIKE '%test%'
  AND fecha_creacion > '0000-00-00 00:00:00'
  AND fecha_aprob > :max_fecha_aprobacion

UNION

SELECT 
    cedula, 
	 anos AS vigencia, 
	 fecha_aprob AS fecha_aprobacion, 
	 fecha_emision,
	 'FIRMA ELECTRONICA' AS  producto,
	 case when estado_me = 1 then 'Aprobado'
	 when estado_me = 5 then 'Emitida'
	 END AS estado_firma,
	 
	 medio AS mediocam,
    razon_social AS razon_social, 
	 ruc_empresa AS ruc, 
	 'ME' AS tipofirma, 
	 SERIAL AS serial_firma, 
    fecha_caduca_v2 AS "fecha_caducidad", 
    correo, 
	 telefono, 
	 cONCAT(nombre," ", ap1, " ", ap2) AS Nombre
FROM Certificados_Electronicos_Subca1.miembro_empresa
WHERE estado_me IN (1, 5)
  AND nombre NOT LIKE '%prueb%'
  AND ap1 NOT LIKE '%prueb%'
  AND ap2 NOT LIKE '%prueb%'
  AND nombre NOT LIKE '%test%'
  AND nombre != 'OPERADOR SD'
  AND ap1 NOT LIKE '%test%'
  AND ap2 NOT LIKE '%test%'   
  AND fecha_creacion > '0000-00-00 00:00:00'
  AND fecha_aprob > :max_fecha_aprobacion

UNION

SELECT 
	 cedula, 
	 anos AS vigencia, 
	 fecha_aprob AS fecha_aprobacion,
	 fecha_emision, 
	 'FIRMA ELECTRONICA' AS  producto,
	 case when estado_rl = 1 then 'Aprobado'
	 when estado_rl = 5 then 'Emitida'
	 END AS estado_firma,
	 medio AS mediocam,
    razon_social AS razon_social, 
	 ruc_empresa AS ruc, 
	 'RL' AS tipofirma,  
	 SERIAL AS serial_firma, 
    fecha_caduca_v2 AS "fecha_caducidad", 
    correo, 
	 telefono, 
	 CONCAT(nombre," ", ap1, " ", ap2) AS Nombre
FROM Certificados_Electronicos_Subca1.representante_legal
WHERE estado_rl IN (1, 5)
  AND nombre NOT LIKE '%prueb%'
  AND ap1 NOT LIKE '%prueb%'
  AND ap2 NOT LIKE '%prueb%'
  AND nombre NOT LIKE '%test%'
  AND ap1 NOT LIKE '%test%'
  AND ap2 NOT LIKE '%test%'
  AND fecha_creacion > '0000-00-00 00:00:00'
  AND fecha_aprob > :max_fecha_aprobacion
  
ORDER BY fecha_caducidad


