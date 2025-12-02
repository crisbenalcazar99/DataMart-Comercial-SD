SELECT 
    cedula, 
	 anos AS vigencia, 
	 fecha_aprob AS fecha_aprobacion, 
	 fecha_emision,
	 medio AS mediocam, 
    '' AS razon_social, 
	 ruc_pn AS ruc, 
	 'PN' AS tipo_firma,
	 SERIAL AS serial_firma, 
    correo, 
	 telefono, 
	 cONCAT(nombre," ", ap1, " ", ap2) AS nombre,
	     CASE 
        WHEN fechaExpiracion IS NULL AND anos LIKE '%M%' AND estado_pn = 5 AND fecha_aprob > '0000-00-00 00:00:00' THEN TIMESTAMPADD(month, 6, fecha_aprob)
        WHEN fechaExpiracion IS NULL AND anos NOT LIKE '%M%' AND estado_pn = 5 AND fecha_aprob > '0000-00-00 00:00:00' THEN TIMESTAMPADD(year, anos, fecha_aprob)
        WHEN fechaExpiracion IS NULL AND anos LIKE '%M%' AND estado_pn = 1 AND fecha_emision > '0000-00-00 00:00:00' THEN TIMESTAMPADD(month, 6, fecha_emision)
        WHEN fechaExpiracion IS NULL AND anos NOT LIKE '%M%' AND estado_pn = 1 AND fecha_emision > '0000-00-00 00:00:00' THEN TIMESTAMPADD(year, anos, fecha_emision)
        WHEN fechaExpiracion IS NOT NULL THEN fechaExpiracion
    END AS "fecha_caducidad",
    CASE 
      WHEN (sf_control = 0) then 'FIRMA ELECTRONICA'
	 	WHEN (sf_control = 1) then 'SF CON FIRMA' 
	 end as producto,
	 case when estado_pn = 1 then 'Aprobado'
	 when estado_pn = 5 then 'Emitida'
	 END AS estado_firma
	 
FROM Certificados_Electronicos_Subca2.persona_natural
WHERE estado_pn IN (1, 5) 
  AND nombre NOT LIKE '%prueb%'
  AND ap1 NOT LIKE '%prueb%'
  AND ap2 NOT LIKE '%prueb%'
  AND nombre != 'OPERADOR SD'
  AND nombre NOT like 'OPERADOR%'
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
	 medio AS mediocam,
    razon_social AS razon_social, 
	 ruc_empresa AS ruc, 
	 'ME' AS tipofirma, 
	 SERIAL AS serial_firma,  
    correo, 
	 telefono, 
	 cONCAT(nombre," ", ap1, " ", ap2) AS nombre,
	     CASE 
        WHEN fechaExpiracion IS NULL AND anos LIKE '%M%' AND estado_me = 5 AND fecha_aprob > '0000-00-00 00:00:00' THEN TIMESTAMPADD(month, 6, fecha_aprob)
        WHEN fechaExpiracion IS NULL AND anos NOT LIKE '%M%' AND estado_me = 5 AND fecha_aprob > '0000-00-00 00:00:00' THEN TIMESTAMPADD(year, anos, fecha_aprob)
        WHEN fechaExpiracion IS NULL AND anos LIKE '%M%' AND estado_me = 1 AND fecha_emision > '0000-00-00 00:00:00' THEN TIMESTAMPADD(month, 6, fecha_emision)
        WHEN fechaExpiracion IS NULL AND anos NOT LIKE '%M%' AND estado_me = 1 AND fecha_emision > '0000-00-00 00:00:00' THEN TIMESTAMPADD(year, anos, fecha_emision)
        WHEN fechaExpiracion IS NOT NULL THEN fechaExpiracion
    END AS "fecha_caducidad",
    CASE 
      WHEN (sf_control = 0) then 'FIRMA ELECTRONICA'
	 	WHEN (sf_control = 1) then 'SF CON FIRMA'
		end as producto,
	 case when estado_me = 1 then 'Aprobado'
	 when estado_me = 5 then 'Emitida'
	 END AS estado_firma
FROM Certificados_Electronicos_Subca2.miembro_empresa
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
	 medio AS mediocam,
    razon_social AS razon_social, 
	 ruc_empresa AS ruc, 
	 'RL' AS tipofirma,  
	 SERIAL AS serial_firma, 
    correo, 
	 telefono, 
	 CONCAT(nombre," ", ap1, " ", ap2) AS nombre,
	     CASE 
        WHEN fechaExpiracion IS NULL AND anos LIKE '%M%' AND estado_rl = 5 AND fecha_aprob > '0000-00-00 00:00:00' THEN TIMESTAMPADD(month, 6, fecha_aprob)
        WHEN fechaExpiracion IS NULL AND anos NOT LIKE '%M%' AND estado_rl = 5 AND fecha_aprob > '0000-00-00 00:00:00' THEN TIMESTAMPADD(year, anos, fecha_aprob)
        WHEN fechaExpiracion IS NULL AND anos LIKE '%M%' AND estado_rl = 1 AND fecha_emision > '0000-00-00 00:00:00' THEN TIMESTAMPADD(month, 6, fecha_emision)
        WHEN fechaExpiracion IS NULL AND anos NOT LIKE '%M%' AND estado_rl = 1 AND fecha_emision > '0000-00-00 00:00:00' THEN TIMESTAMPADD(year, anos, fecha_emision)
        WHEN fechaExpiracion IS NOT NULL THEN fechaExpiracion
    END AS "fecha_caducidad",
    CASE 
      WHEN (sf_control = 0) then 'FIRMA ELECTRONICA'
	 	WHEN (sf_control = 1) then 'SF CON FIRMA'
		end as producto,
	 case when estado_rl = 1 then 'Aprobado'
	 when estado_rl = 5 then 'Emitida'
	 END AS estado_firma
FROM Certificados_Electronicos_Subca2.representante_legal
WHERE estado_rl IN (1, 5)
  AND nombre NOT LIKE '%prueb%'
  AND ap1 NOT LIKE '%prueb%'
  AND ap2 NOT LIKE '%prueb%'
  AND nombre NOT LIKE '%test%'
  AND ap1 NOT LIKE '%test%'
  AND ap2 NOT LIKE '%test%'
  AND fecha_creacion > '0000-00-00 00:00:00'
  AND fecha_aprob > :max_fecha_aprobacion
  
UNION

SELECT 
	 cedula, 
	 vigenciaServicio AS vigencia, 
	 fechaAprob AS fecha_aprobacion, 
	 NULL AS fecha_emision,
	 'SF SF' AS mediocam,
    razon_social AS razon_social, 
	 ruc, 
	 null AS tipofirma,  
	 CONCAT(ruc,'-',fechaAprob) as serial_firma, 
    correo, 
	 telefono, 
	 nombres AS nombre,
    TIMESTAMPADD(year, vigenciaServicio, fechaAprob) AS "fecha_caducidad",
    "SF SIN FIRMA" producto,
	 case when estado = 1 then 'Aprobado'
	 when estado = 5 then 'Emitida'
	 END AS estado_firma
FROM Certificados_Electronicos_Subca2.tb_securityFSinFirma
WHERE estado IN (1, 5)
  AND nombres NOT LIKE '%prueb%'
  AND nombres NOT LIKE '%test%'
  AND fecha_creacion > '0000-00-00 00:00:00'  
  AND fechaAprob IS NOT NULL 
  AND fechaAprob > :max_fecha_aprobacion


