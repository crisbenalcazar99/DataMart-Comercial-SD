SELECT 
	 u.username as cedula,
	 c7.numero_vigencia AS vigencia, 
	 fs.fecha_aprobacion::timestamp without time ZONE,
	 CASE WHEN fs.fecha_emision = '' THEN NULL ELSE fs.fecha_emision::TIMESTAMP WITHOUT TIME ZONE END AS fecha_emision,
	 c8.nombre AS producto, 
	 c8.nombre AS producto_especifico,
	 c5.valor AS mediocam, 
    ds.razon_social_empresa_representante_legal AS "razon_social",
	 (select case WHEN ds.tipo_persona=521 THEN ds.numero_ruc ELSE ds.ruc_empresa_representante_legal END) AS "ruc", 
	 c2.valor AS tipo_firma,
    c_10.nombre AS estado_firma,
	 fs.serial_firma,
    CASE WHEN u.email IS NOT NULL THEN u.email ELSE ds.correo end AS correo, 
	 (SELECT CASE WHEN u.telefono_contacto IS NOT NULL then u.telefono_contacto ELSE u.telefono END) AS "telefono",
	 u.nombre, 
	 u.apellido_paterno,
	 u.apellido_materno,
	 fs.fecha_expiracion::TIMESTAMP WITHOUT TIME ZONE AS fecha_caducidad,
	 CASE 
	 	WHEN u_1.username IS NOT NULL THEN u_1.username 
	 	WHEN u_2.username IS NOT NULL THEN u_2.username
		 ELSE 'CAMUNDA' end AS "operador_creacion",
	 u.fecha_nacimiento,
	 u.profesion,
	 ds.actividad_ruc,
	 ds.clase_contribuyente,
	 ds.sector_economico,
	 CASE WHEN ds.contribuyente_fantasma = 'NO' THEN FALSE WHEN ds.contribuyente_fantasma = 'SI' THEN TRUE ELSE NULL END AS contribuyente_fantasma,
	 fs.id_firma AS link_id_firma,
	 t.id_tramite,
	 ds.provincia AS id_provincia,
    ds.canton    AS id_canton,
    ds.parroquia AS id_parroquia,
    c_3.nombre AS "medio_contacto",
    c_4.nombre AS "tipo_atencion",
    CASE 
	 	WHEN u_1.username IS NOT NULL THEN c_5.nombre 
		when c_6.nombre IS NOT NULL THEN c_6.nombre 
		ELSE 'Grupo Security Data' end AS grupo_operador,
	 ref_puntos.puntos AS security_points 

	 
FROM tramite t
JOIN users u ON u.id = t.id_user
JOIN public.firma_electronica fe ON t.id_tramite = fe.id_tramite
JOIN public.catalogo c7 ON fe.anios_vigencia = c7.codigo_catalogo
JOIN public.catalogo c8 ON t.id_proceso = c8.codigo_catalogo
JOIN public.datos_solicitante ds ON t.id_tramite = ds.id_tramite
JOIN public.firma_subida fs ON fs.id_tramite = t.id_tramite
JOIN public.catalogo c2 ON ds.tipo_persona = c2.codigo_catalogo
JOIN public.catalogo c_10 ON c_10.codigo_catalogo = fs.estado_certificado
JOIN public.catalogo c5 ON fe.tipo_contenedor = c5.codigo_catalogo
JOIN PUBLIC.solicitud s ON t.id_tramite = s.id_tramite
LEFT JOIN PUBLIC.users u_1 ON s.id_operador_creacion =  u_1.id
LEFT Join PUBLIC.catalogo c_3 ON c_3.codigo_catalogo = s.id_medio_contacto
JOIN PUBLIC.catalogo c_4 ON c_4.codigo_catalogo = ds.tipo_atencion
LEFT Join PUBLIC.catalogo c_5 ON c_5.codigo_catalogo = u_1.grupo 
LEFT JOIN PUBLIC.users u_2 ON s.id_operador =  u_2.id
LEFT Join PUBLIC.catalogo c_6 ON c_6.codigo_catalogo = u_2.grupo 
left JOIN PUBLIC.ref_puntos_referidos ref_puntos 
  ON t.id_user = ref_puntos.id_user
  AND ref_puntos.estado_registro = TRUE
  AND ref_puntos.status = 11111



WHERE 
fs.estado_registro = TRUE
AND t.estado_registro = TRUE
AND fe.estado_registro = TRUE 
AND s.estado_registro = TRUE
AND t.fecha_inicio_tramite >= '2024-01-13' 
AND t.id_proceso IN (482, 481, 491, 495, 487, 489, 491, 495)
AND t.id_tarea != 627
AND fs.estado_certificado NOT IN (266, 269)
AND u.tipo != 781
AND fs.fecha_aprobacion NOT IN ('', '0000-00-00 00:00:00')
AND fs.fecha_aprobacion::timestamp without time ZONE > :max_fecha_aprobacion




UNION

SELECT 
	 u.username AS cedula,
	 null AS vigencia, 
	 sf.fecha_registro as fecha_aprobacion,
	 sf.fecha_registro AS fecha_emision,
	 c8.nombre AS producto, 
	 c8.nombre AS producto_especifico,
	 'SF SF' AS mediocam, 
    ds.razon_social_empresa_representante_legal AS "razon_social",
	 (select case WHEN ds.tipo_persona=521 THEN ds.numero_ruc ELSE ds.ruc_empresa_representante_legal END) AS "ruc", 
	 c2.valor AS tipo_firma,
    NULL  AS estado_firma,
	 (select case WHEN ds.tipo_persona=521 THEN ds.numero_ruc ELSE ds.ruc_empresa_representante_legal END || '-' || sf.fecha_registro) as serial_firma,
    CASE WHEN u.email IS NOT NULL THEN u.email ELSE ds.correo end AS correo, 
	 (SELECT CASE WHEN u.telefono_contacto IS NOT NULL then u.telefono_contacto ELSE u.telefono END) AS "telefono",
	 u.nombre, 
	 u.apellido_paterno,
	 u.apellido_materno,
	 (LEFT(sf.fecha_expiracion, 10) || ' 05:00:00')::TIMESTAMP WITHOUT TIME ZONE  AS fecha_caducidad,
	 CASE 
	 	WHEN u_1.username IS NOT NULL THEN u_1.username 
	 	WHEN u_2.username IS NOT NULL THEN u_2.username
		 ELSE 'CAMUNDA' end AS "operador_creacion",
	 u.fecha_nacimiento,
	 u.profesion,
	 ds.actividad_ruc,
	 ds.clase_contribuyente,
	 ds.sector_economico,
	 CASE WHEN ds.contribuyente_fantasma = 'NO' THEN FALSE WHEN ds.contribuyente_fantasma = 'SI' THEN TRUE ELSE NULL END AS contribuyente_fantasma,
	 NULL AS link_id_firma,
	 t.id_tramite,
    ds.provincia AS id_provincia,
    ds.canton    AS id_canton,
    ds.parroquia AS id_parroquia,
    c_3.nombre AS "medio_contacto",
    c_4.nombre AS "tipo_atencion",
    CASE 
	 	WHEN u_1.username IS NOT NULL THEN c_5.nombre 
		when c_6.nombre IS NOT NULL THEN c_6.nombre 
		ELSE 'Grupo Security Data' end AS grupo_operador,
		 ref_puntos.puntos AS security_points 
    
FROM tramite t
JOIN users u ON u.id = t.id_user
JOIN security_factura sf ON sf.id_tramite = t.id_tramite 
JOIN public.catalogo c8 ON t.id_proceso = c8.codigo_catalogo
JOIN public.datos_solicitante ds ON t.id_tramite = ds.id_tramite
JOIN public.catalogo c2 ON ds.tipo_persona = c2.codigo_catalogo
JOIN PUBLIC.solicitud s ON t.id_tramite = s.id_tramite
LEFT JOIN PUBLIC.users u_1 ON s.id_operador_creacion =  u_1.id
LEFT Join PUBLIC.catalogo c_5 ON c_5.codigo_catalogo = u_1.grupo 
LEFT JOIN PUBLIC.catalogo c_3 ON c_3.codigo_catalogo = s.id_medio_contacto
JOIN PUBLIC.catalogo c_4 ON c_4.codigo_catalogo = ds.tipo_atencion
LEFT JOIN PUBLIC.users u_2 ON s.id_operador =  u_2.id
LEFT Join PUBLIC.catalogo c_6 ON c_6.codigo_catalogo = u_2.grupo 
left JOIN PUBLIC.ref_puntos_referidos ref_puntos 
  ON t.id_user = ref_puntos.id_user
 AND ref_puntos.estado_registro = TRUE
 AND ref_puntos.status = 11111



WHERE 
sf.estado_registro = TRUE 
AND t.estado_registro = TRUE
AND s.estado_registro = TRUE

AND t.fecha_inicio_tramite >= '2024-01-13'
AND t.id_proceso IN (488)
AND t.id_tarea != 627
AND u.tipo != 781
AND sf.fecha_expiracion != ''
AND sf.fecha_expiracion != '0000/00/00'
AND sf.fecha_registro > :max_fecha_aprobacion


 ORDER BY vigencia




