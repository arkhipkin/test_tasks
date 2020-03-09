CREATE TABLE address
(
    id integer,
    company_id text COLLATE pg_catalog."default",
    country character varying(255) COLLATE pg_catalog."default",
    postal_code character varying(255) COLLATE pg_catalog."default",
    city character varying(255) COLLATE pg_catalog."default",
    district character varying(255) COLLATE pg_catalog."default",
    street character varying(255) COLLATE pg_catalog."default",
    street_number character varying(255) COLLATE pg_catalog."default",
    addition character varying(255) COLLATE pg_catalog."default",
    created_at date
);

CREATE TABLE company
(
    company_id text COLLATE pg_catalog."default",
    status character varying(255) COLLATE pg_catalog."default",
    rating_threshold character varying(255) COLLATE pg_catalog."default",
    company_name text COLLATE pg_catalog."default",
    foundation_date date,
    legal_form character varying(255) COLLATE pg_catalog."default",
    created_at date
);


INSERT INTO public.address (id,company_id,country,postal_code,city,district,street,street_number,addition,created_at) VALUES
(63,'0ac1588e2b19d3887b0049bda5d79cdc','DE','55767','Rötsweiler-Nockenthal','','','','','2019-06-01')
,(1020,'c8d72cd256de2e759c6bc752cf5b8ba8','DE','','Näfels/Glarus,Schweiz','','','','','2019-06-01')
,(1021,'fb40388bbe6eeb278eb412fa8ada288a','DE','','Bielefeld','','','','','2019-06-01')
,(1344,'49dfe466232a5b11a41ebbe0d2ba4063','DE','','Zürich/Schweiz','','','','','2019-06-01')
,(330,'5cf807d2e4c4bd5114817f106fa269d0','DE','33803','Steinhagen','','','','','2019-06-01')
,(1848,'49a1222d8dd96bfd91898aab2d43d6b6','DE','','Bielefeld','','SITZ: ESSEN','','','2019-06-01')
,(646,'8cf68dad9c0fd134ad5db1de0c755ecb','DE','33775','Versmold','','','','','2019-06-01')
,(246,'be14b1f1cd3b2476a0743e05343c2fc7','DE','','Bielefeld','','','','','2019-06-01')
,(1686,'7e212a304ea58f23c3412d1ecd68ba69','DE','63762','Großostheim','','','','','2019-06-01')
,(1,'c13083f66d1945e04b3a4a9c2b174b67','DE','55576','Sprendlingen','','Kreuznacher Str.','80','','2019-06-01')
;
INSERT INTO public.address (id,company_id,country,postal_code,city,district,street,street_number,addition,created_at) VALUES 
(2,'bb092461067b4b52cf56849d58fc3129','DE','55767','Schwollen','','Hauptstr.','36','','2019-06-01')
,(3,'6cecf41bf99afbaead9ccdf399921321','DE','55422','Bacharach','','Oberstr.','22','','2019-06-01')
,(4,'5079fed2c4a106471e1d87909db6dc75','DE','33649','Bielefeld','','Forstweg','23','','2019-06-01')
,(5,'e73172cda318c5fc28677e57fcd19579','DE','33803','Steinhagen','Brockhagen','Körnerstr.','5','','2019-06-01')
,(6,'0a643253602834f6708b486954e0bdd4','DE','55487','Sohren','','Industriestr.','9','','2019-06-01')
,(7,'62db30ed6ac1d5f02b3e8c5cdad58b24','DE','55411','Bingen','','Stefan-George-Str.','20','','2019-06-01')
,(8,'8cdb490bcad0848ce4ab533550b9e3e9','DE','55543','Bad Kreuznach','','Bosenheimer Str.','200','a','2019-06-01')
,(9,'d21a8148fa34cd5c04f49656dbbe5099','DE','55743','Idar-Oberstein','','Am Rumpelteich','5','','2019-06-01')
,(10,'11038a579e3de864fe4981443f6ffec6','DE','55566','Bad Sobernheim','','Großstr.','47','','2019-06-01')
,(11,'b961bbff770b268be22de3134001006c','DE','55422','Bacharach','','Oberstr.','38','','2019-06-01')
;
INSERT INTO public.address (id,company_id,country,postal_code,city,district,street,street_number,addition,created_at) VALUES 
(13,'0bece7e818afc9a97a1c6a415a540e92','DE','55442','Stromberg','','Am Markt','5','','2019-06-01')
,(12,'d7d55c5eac26b48cddc78f1ac98b648a','DE','55422','Bacharach','','Langstr.','18','','2019-06-01')
,(14,'6612c0ba1073f73aa2d9ae83305fca26','DE','55422','Bacharach','','Langstr.','30','','2019-06-01')
,(15,'c366d522c412e86aa908a95b60e8295e','DE','33689','Bielefeld','Sennestadt','Fuggerstr.','15','','2019-06-01')
,(16,'821da9faab9b1b01fd8a78cf8478f7b4','DE','55422','Bacharach','','Blücherstr.','250','','2019-06-01')
,(17,'0e49ddbbd5d8023f4f773abc703b8a84','DE','65375','Oestrich-Winkel','','Rheingaustr.','19','a','2019-06-01')
,(18,'bfdb362cbac58be2165a9e580d20afe4','DE','55543','Bad Kreuznach','','Wöllsteiner Str.','8','','2019-06-01')
,(20,'995a6c275e91ebec1ca008b3237afc02','DE','33604','Bielefeld','Sieker','Lipper Hellweg','49','','2019-06-01')
,(21,'79c3e0dc738bbd86c7b6ae6b8dc0e440','DE','55743','Idar-Oberstein','','Struthstr.','29','','2019-06-01')
,(22,'27dec403f2df38a0ccc4e202459db8b3','DE','33790','Halle','','Flurstr.','35','','2019-06-01')
;
INSERT INTO public.address (id,company_id,country,postal_code,city,district,street,street_number,addition,created_at) VALUES 
(23,'22c9f7e30e1395979c38aeb8c1a2ed8c','DE','55413','Weiler','','Grabenstr.','19','','2019-06-01')
,(24,'dfea0af6a9f8875f26ad1c1fa4a88bd6','DE','33758','Schloß Holte-Stukenbrock','','Hellweg','184','-194','2019-06-01')
,(25,'c58dcba5fcf717b627f867e541c9e98f','DE','55585','Altenbamberg','','Bruchwiese','1','','2019-06-01')
,(27,'dcd94d503b28c8de8bfeb74d9a0e9282','DE','33790','Halle','','Leimweg','16','','2019-06-01')
,(28,'4ba6104f89b44dbd563028079fdf70c0','DE','55457','Horrweiler','','Gensinger Str.','44','','2019-06-01')
,(29,'d140f15d8a9a68ca3e3b6c964968b417','DE','33719','Bielefeld','','Ludwig-Steil-Str.','10','','2019-06-01')
,(30,'32412f10c63c90c29c6c6e1edb941439','DE','55546','Hackenheim','','Bosenheimer Str.','9','','2019-06-01')
,(31,'6c3abeba0076ac92e27998699de7d81e','DE','33758','Schloß Holte-Stukenbrock','Schloß Holte','Weißer Weg','4','','2019-06-01')
,(19,'a9522c1552ff019313ee7f7c0d37f243','DE','33615','Bielefeld','','Kurt-Schumacher-Str.','24','','2019-06-01')
,(32,'d84f7fdd8cdd0346a81476a78df21dc2','DE','33613','Bielefeld','','Jöllenbecker Str.','289','','2019-06-01')
;
INSERT INTO public.address (id,company_id,country,postal_code,city,district,street,street_number,addition,created_at) VALUES 
(33,'350241896ed9801acca7de1819e56f55','DE','33611','Bielefeld','Schildesche','Ernst-Graebe-Str.','10','','2019-06-01')
,(34,'aa7dbc641ec2fc88c609224b84dceda9','DE','55452','Guldental','','Naheweinstr.','91','-93','2019-06-01')
,(35,'8a492ec4e9527a90725f7e78c6f75ec9','DE','33611','Bielefeld','Schildesche','Ernst-Graebe-Str.','10','','2019-06-01')
,(36,'90f30d6231d2ccbfa9ea77a019153185','DE','55758','Sien','','Kirner Str.','6','','2019-06-01')
,(37,'5659068f2fafdc567e4e6672d538dc70','DE','55481','Kirchberg','','Graf-Simon-Str.','28','','2019-06-01')
,(39,'eaae316348d8cbcd3960bef0da852d05','DE','56290','Dommershausen','Sabershausen','Stilweg','10','','2019-06-01')
,(38,'2116df0c7a3902215d8817da02342b0e','DE','55626','Bundenbach','','Ringstr.','23','','2019-06-01')
,(40,'5a626c6efed2d8140dd475eadae80f0f','DE','55743','Idar-Oberstein','','Amtsstr.','16','','2019-06-01')
,(41,'326d8b84480a51c7e4395ca245017bed','DE','55422','Bacharach','','Mainzer Str.','24','','2019-06-01')
,(42,'471c318196e8937558e7fe76d619e802','DE','55606','Kirn','','Friedrichstr.','2','','2019-06-01')
;
INSERT INTO public.address (id,company_id,country,postal_code,city,district,street,street_number,addition,created_at) VALUES 
(43,'7f0955e0ab83795c128530625ea173d4','DE','33829','Borgholzhausen','','Südstr.','2','','2019-06-01')
,(44,'3018b87f8c3ef13f275333a68f49ee44','DE','55606','Kirn','','Friedrichstr.','2','','2019-06-01')
,(45,'a4a45f1ed06383542a2bc5a60d0a924e','DE','33602','Bielefeld','','Niederwall','32','','2019-06-01')
,(46,'cc3bb127da80beb7408f61eb3820e951','DE','55743','Idar-Oberstein','','Auf der Burr','28','','2019-06-01')
,(47,'07ba0b2165c01e34b93d2a892aaddb40','DE','55606','Kirn','','Binger Landstr.','37','b','2019-06-01')
,(48,'d6afec8f3f57480b62f1b53521bc235c','DE','55743','Idar-Oberstein','','Hauptstr.','335','','2019-06-01')
,(49,'b10f63419d97f20fdbff273641c04e19','DE','33790','Halle','Bokel','Gütersloher Str.','7','','2019-06-01')
,(50,'f64ed7bd45f813ec0d711b3bf17512ae','DE','55743','Idar-Oberstein','','Frühaufstr.','4','','2019-06-01')
,(51,'a6e22ec9ee46cfc046c06b1e3007dffe','DE','55743','Idar-Oberstein','','Seitzenbachstr.','1','','2019-06-01')
,(52,'1fbd1f2f50c8d379bcec8e39e41d84fe','DE','33609','Bielefeld','','An der Pottenau','7','','2019-06-01')
;
INSERT INTO public.address (id,company_id,country,postal_code,city,district,street,street_number,addition,created_at) VALUES 
(53,'7f5dea29def9f991ee7f24156ca210e5','DE','33604','Bielefeld','','Andreas-Lamey-Str.','6','','2019-06-01')
,(54,'5a7cf51858a3afea4363d9ec9c8bef3b','DE','33649','Bielefeld','Brackwede','Brockhagener Str.','194','','2019-06-01')
,(55,'843a94a7cef2916869fba0b23c191519','DE','33619','Bielefeld','','Katzenstr.','1','','2019-06-01')
,(56,'594dcfd568ec523bbd53e06047dd5b75','DE','55546','Fürfeld','','Kreuznacher Str.','39','','2019-06-01')
,(57,'0826a1e96bf60d6a00f59788d1f90bc7','DE','56290','Dommershausen','','Hauptstr.','16','','2019-06-01')
,(58,'29635deedbf4e75cd30297bfbe37cb68','DE','55626','Bundenbach','','Burgweg','18','','2019-06-01')
,(59,'9b33fb5b804d2728c8966007a3dcb1ec','DE','33659','Bielefeld','','Krackser Str.','12','','2019-06-01')
,(60,'4ef8a9bed1981126bad12f7eb3956e40','DE','65385','Rüdesheim am Rhein','','Rheinstr.','3','','2019-06-01')
,(61,'68aadd6671af61227d4504ed9eef548a','DE','33729','Bielefeld','','Mecklenburger Str.','21','','2019-06-01')
,(62,'aa2500757493a9476929a0a775da3d96','DE','33659','Bielefeld','','Krackser Str.','12','','2019-06-01')
;
INSERT INTO public.address (id,company_id,country,postal_code,city,district,street,street_number,addition,created_at) VALUES 
(65,'d9ee2338b9d641e92b3c5da75dc3d489','DE','33649','Bielefeld','Ummeln','Dammweg','4','','2019-06-01')
,(78,'0b5b6d797614ba19e96f3c69c58c97f7','DE','33729','Bielefeld','Altenhagen','Vinner Str.','157','','2019-06-01')
,(306,'64ee8652ed616a9623c0f5c6ca69dd45','DE','33607','Bielefeld','Innenstadt','Am Strebkamp','14','','2019-06-01')
,(313,'fedffaecb3a228c183bc9fe10e5f5d6a','DE','33824','Werther','','Engerstr.','28','','2019-06-01')
,(316,'caf8a36b30f664bfd716e98a6b2e978e','DE','33699','Bielefeld','Ubbedissen','Neue Reihe','2','','2019-06-01')
,(323,'7ae63f7e98937be13ec7c9215bde9737','DE','33699','Bielefeld','','Linnenstr.','','','2019-06-01')
,(327,'cfa8a7663ded8585889ab119fb3d9823','DE','33604','Bielefeld','','Otto-Brenner-Str.','206','','2019-06-01')
,(332,'f115e8846cf8b21fe5651fb4be8631dd','DE','33604','Bielefeld','','Oststr.','10','b','2019-06-01')
,(404,'37e6aece15b2e7585b0ffce3fb2d4a1c','DE','33729','Bielefeld','Altenhagen','Brönninghauser Str.','45','','2019-06-01')
,(413,'373903b8c14dda1288ca021eede880d8','DE','33607','Bielefeld','Innenstadt','Meisenstr.','94','','2019-06-01')
;
INSERT INTO public.address (id,company_id,country,postal_code,city,district,street,street_number,addition,created_at) VALUES 
(422,'df6d86f8772b34c43505034466a8876e','DE','33604','Bielefeld','Innenstadt','Eduard-Windthorst-Str.','4','','2019-06-01')
,(424,'accc612913163fc822042096c8511311','DE','33790','Halle','','Maschweg','61','','2019-06-01')
,(453,'585f245e69e0dbb3df4e392365887cfc','DE','33607','Bielefeld','','Am Großen Holz','20','','2019-06-01')
,(489,'32264310ee08356d0755589a3d0e7547','DE','33609','Bielefeld','Baumheide','Schelpmilser Weg','6','','2019-06-01')
,(563,'f3b98eb6e610fb0d4b03cb0eb20916d9','DE','33615','Bielefeld','Innenstadt','Große-Kurfürsten-Str.','41','a','2019-06-01')
,(637,'506b40d16ccac41247c9a47f0bf42dfb','DE','33615','Bielefeld','','Friedrichstr.','3','','2019-06-01')
,(699,'6e683892d432532f8d79cf3e99fe1cdc','DE','33602','Bielefeld','','Siekerwall','21','','2019-06-01')
,(859,'682c2f8e52dec3a66f2d4932d088a83b','DE','33719','Bielefeld','Oldentrup','Potsdamer Str.','190','','2019-06-01')
,(1443,'5ed85e8e6e6a98bd8465832848cb67ee','DE','33729','Bielefeld','Brake','Grafenheider Str.','96','a','2019-06-01')
,(1767,'5c8a9d612f73c10b1620a8db801d3873','DE','33609','Bielefeld','Innenstadt','Hakenort','47','','2019-06-01')
;
INSERT INTO public.address (id,company_id,country,postal_code,city,district,street,street_number,addition,created_at) VALUES 
(1855,'cbe3afef55ae8bc74002682aa62e7e62','DE','33615','Bielefeld','','Wertherstr.','128','a','2019-06-01')
,(1931,'3d170269c5942f5be83554269f604c4b','DE','33719','Bielefeld','Oldentrup','Ludwig-Erhard-Allee','39','','2019-06-01')
,(1944,'84667c37ca4d6373f5c36200759b52f1','DE','33602','Bielefeld','Innenstadt','Oberntorwall','23','c','2019-06-01')
,(2297,'c01a1dee68d51d749e22c95c07892217','DE','33790','Halle','','Kiskerstr.','1','','2019-06-01')
,(2311,'56d7fbb3f3427109eab58a718e61c6aa','DE','33602','Bielefeld','','Goldstr.','2','','2019-06-01')
,(2320,'a655a26861ac750a3cfbc352e6725e1a','DE','33659','Bielefeld','','Zeppelinstr.','1','','2019-06-01')
,(2334,'9b5e6d0170b873b599b7a30138b3698c','DE','33602','Bielefeld','','Niedernstr.','18','','2019-06-01')
,(2339,'a457840bb70fb8faa00fd78be1e98ea4','DE','33649','Bielefeld','','Archimedesstr.','1','-4','2019-06-01')
,(2343,'ed0a8c95f2ea89cfc0032bb7065b1fe5','DE','33729','Bielefeld','Altenhagen','Kafkastr.','42','','2019-06-01')
;

INSERT INTO public.company (company_id,status,rating_threshold,company_name,foundation_date,legal_form,created_at) VALUES
('0ac1588e2b19d3887b0049bda5d79cdc','aktiv','FALSE','company_name0ac1588e2b19d3887b0049bda5d79cdc','1976-01-01','Verein','2019-06-01')
,('c8d72cd256de2e759c6bc752cf5b8ba8','aktiv','UNKNOWN','company_namec8d72cd256de2e759c6bc752cf5b8ba8','1980-01-01','AG','2019-06-01')
,('fb40388bbe6eeb278eb412fa8ada288a','aktiv','UNKNOWN','company_namefb40388bbe6eeb278eb412fa8ada288a','1983-07-05','Verein','2019-06-01')
,('49dfe466232a5b11a41ebbe0d2ba4063','aktiv','UNKNOWN','company_name49dfe466232a5b11a41ebbe0d2ba4063','1900-01-01','AG','2019-06-01')
,('5cf807d2e4c4bd5114817f106fa269d0','aktiv','UNKNOWN','company_name5cf807d2e4c4bd5114817f106fa269d0','1965-01-01','GmbH & Co. KG','2019-06-01')
,('49a1222d8dd96bfd91898aab2d43d6b6','aktiv','FALSE','company_name49a1222d8dd96bfd91898aab2d43d6b6','1870-05-27','AG','2019-06-01')
,('8cf68dad9c0fd134ad5db1de0c755ecb','aktiv','UNKNOWN','company_name8cf68dad9c0fd134ad5db1de0c755ecb','1900-01-01','GmbH & Co. KG','2019-06-01')
,('be14b1f1cd3b2476a0743e05343c2fc7','aktiv','UNKNOWN','company_namebe14b1f1cd3b2476a0743e05343c2fc7','1977-04-28','GmbH','2019-06-01')
,('7e212a304ea58f23c3412d1ecd68ba69','aktiv','UNKNOWN','company_name7e212a304ea58f23c3412d1ecd68ba69',NULL,'Freie Berufe','2019-06-01')
,('c13083f66d1945e04b3a4a9c2b174b67','eingestellt / ruhend','TRUE','company_namec13083f66d1945e04b3a4a9c2b174b67','1935-10-01','GmbH & Co. KG','2019-06-01')
;
INSERT INTO public.company (company_id,status,rating_threshold,company_name,foundation_date,legal_form,created_at) VALUES
('bb092461067b4b52cf56849d58fc3129','aktiv','FALSE','company_namebb092461067b4b52cf56849d58fc3129','1972-01-01','GmbH','2019-06-01')
,('6cecf41bf99afbaead9ccdf399921321','aktiv','FALSE','company_name6cecf41bf99afbaead9ccdf399921321','1964-05-11','GbR','2019-06-01')
,('5079fed2c4a106471e1d87909db6dc75','aktiv','FALSE','company_name5079fed2c4a106471e1d87909db6dc75','1977-01-01','Gewerbebetrieb','2019-06-01')
,('e73172cda318c5fc28677e57fcd19579','aufgelöst','TRUE','company_namee73172cda318c5fc28677e57fcd19579','1969-10-03','GmbH','2019-06-01')
,('0a643253602834f6708b486954e0bdd4','aktiv','FALSE','company_name0a643253602834f6708b486954e0bdd4','1978-12-19','GmbH','2019-06-01')
,('62db30ed6ac1d5f02b3e8c5cdad58b24','aufgelöst','TRUE','company_name62db30ed6ac1d5f02b3e8c5cdad58b24','1979-12-17','GmbH','2019-06-01')
,('8cdb490bcad0848ce4ab533550b9e3e9','eingestellt / ruhend','UNKNOWN','company_name8cdb490bcad0848ce4ab533550b9e3e9','1977-01-01','GmbH','2019-06-01')
,('d21a8148fa34cd5c04f49656dbbe5099','aktiv','FALSE','company_named21a8148fa34cd5c04f49656dbbe5099','1969-01-01','Gewerbebetrieb','2019-06-01')
,('11038a579e3de864fe4981443f6ffec6','aktiv','FALSE','company_name11038a579e3de864fe4981443f6ffec6','1983-12-01','Gewerbebetrieb','2019-06-01')
,('b961bbff770b268be22de3134001006c','aktiv','FALSE','company_nameb961bbff770b268be22de3134001006c','2001-04-01','Gewerbebetrieb','2019-06-01')
;
INSERT INTO public.company (company_id,status,rating_threshold,company_name,foundation_date,legal_form,created_at) VALUES
('0bece7e818afc9a97a1c6a415a540e92','aktiv','FALSE','company_name0bece7e818afc9a97a1c6a415a540e92','1979-04-01','Gewerbebetrieb','2019-06-01')
,('d7d55c5eac26b48cddc78f1ac98b648a','aktiv','FALSE','company_named7d55c5eac26b48cddc78f1ac98b648a','1954-01-01','Gewerbebetrieb','2019-06-01')
,('6612c0ba1073f73aa2d9ae83305fca26','aktiv','FALSE','company_name6612c0ba1073f73aa2d9ae83305fca26','1982-01-01','Gewerbebetrieb','2019-06-01')
,('c366d522c412e86aa908a95b60e8295e','aktiv','FALSE','company_namec366d522c412e86aa908a95b60e8295e','1977-12-21','GmbH','2019-06-01')
,('821da9faab9b1b01fd8a78cf8478f7b4','aktiv','FALSE','company_name821da9faab9b1b01fd8a78cf8478f7b4','1972-01-01','Gewerbebetrieb','2019-06-01')
,('0e49ddbbd5d8023f4f773abc703b8a84','eingestellt / ruhend','TRUE','company_name0e49ddbbd5d8023f4f773abc703b8a84','1965-06-02','GmbH','2019-06-01')
,('bfdb362cbac58be2165a9e580d20afe4','eingestellt / ruhend','FALSE','company_namebfdb362cbac58be2165a9e580d20afe4','1956-01-01','Einzelfirma','2019-06-01')
,('995a6c275e91ebec1ca008b3237afc02','eingestellt / ruhend','UNKNOWN','company_name995a6c275e91ebec1ca008b3237afc02','1950-01-01','GmbH','2019-06-01')
,('79c3e0dc738bbd86c7b6ae6b8dc0e440','aktiv','TRUE','company_name79c3e0dc738bbd86c7b6ae6b8dc0e440','1976-01-01','Gewerbebetrieb','2019-06-01')
,('27dec403f2df38a0ccc4e202459db8b3','aktiv','FALSE','company_name27dec403f2df38a0ccc4e202459db8b3','1970-04-01','Gewerbebetrieb','2019-06-01')
;
INSERT INTO public.company (company_id,status,rating_threshold,company_name,foundation_date,legal_form,created_at) VALUES
('22c9f7e30e1395979c38aeb8c1a2ed8c','aktiv','FALSE','company_name22c9f7e30e1395979c38aeb8c1a2ed8c','1982-01-02','Gewerbebetrieb','2019-06-01')
,('dfea0af6a9f8875f26ad1c1fa4a88bd6','aufgelöst','FALSE','company_namedfea0af6a9f8875f26ad1c1fa4a88bd6','1913-01-01','GmbH & Co. KG','2019-06-01')
,('c58dcba5fcf717b627f867e541c9e98f','aufgelöst','TRUE','company_namec58dcba5fcf717b627f867e541c9e98f','1957-01-01','GmbH','2019-06-01')
,('dcd94d503b28c8de8bfeb74d9a0e9282','aktiv','UNKNOWN','company_namedcd94d503b28c8de8bfeb74d9a0e9282','1975-07-01','Gewerbebetrieb','2019-06-01')
,('4ba6104f89b44dbd563028079fdf70c0','aktiv','FALSE','company_name4ba6104f89b44dbd563028079fdf70c0','1980-09-01','Einzelfirma','2019-08-09')
,('d140f15d8a9a68ca3e3b6c964968b417','aktiv','FALSE','company_named140f15d8a9a68ca3e3b6c964968b417','1970-07-29','Gewerbebetrieb','2019-06-01')
,('32412f10c63c90c29c6c6e1edb941439','aktiv','FALSE','company_name32412f10c63c90c29c6c6e1edb941439','1969-01-01','Gewerbebetrieb','2019-06-01')
,('6c3abeba0076ac92e27998699de7d81e','aktiv','TRUE','company_name6c3abeba0076ac92e27998699de7d81e','1980-08-01','Gewerbebetrieb','2019-06-01')
,('a9522c1552ff019313ee7f7c0d37f243','aktiv','FALSE','company_namea9522c1552ff019313ee7f7c0d37f243','1970-01-01','Gewerbebetrieb','2019-06-01')
,('d84f7fdd8cdd0346a81476a78df21dc2','aktiv','FALSE','company_named84f7fdd8cdd0346a81476a78df21dc2','1971-01-01','GmbH','2019-08-09')
;
INSERT INTO public.company (company_id,status,rating_threshold,company_name,foundation_date,legal_form,created_at) VALUES
('350241896ed9801acca7de1819e56f55','aktiv','FALSE','company_name350241896ed9801acca7de1819e56f55','1975-01-15','GmbH','2019-06-01')
,('aa7dbc641ec2fc88c609224b84dceda9','aktiv','FALSE','company_nameaa7dbc641ec2fc88c609224b84dceda9','1933-01-01','GmbH','2019-06-01')
,('8a492ec4e9527a90725f7e78c6f75ec9','aktiv','FALSE','company_name8a492ec4e9527a90725f7e78c6f75ec9','1856-01-01','GmbH & Co. KG','2019-06-01')
,('90f30d6231d2ccbfa9ea77a019153185','aktiv','FALSE','company_name90f30d6231d2ccbfa9ea77a019153185','1969-06-18','GmbH','2019-06-01')
,('5659068f2fafdc567e4e6672d538dc70','aktiv','TRUE','company_name5659068f2fafdc567e4e6672d538dc70','1961-01-01','GmbH','2019-06-01')
,('eaae316348d8cbcd3960bef0da852d05','aktiv','FALSE','company_nameeaae316348d8cbcd3960bef0da852d05','1968-01-24','GmbH','2019-06-01')
,('2116df0c7a3902215d8817da02342b0e','aktiv','FALSE','company_name2116df0c7a3902215d8817da02342b0e','1907-01-01','GmbH','2019-08-09')
,('5a626c6efed2d8140dd475eadae80f0f','aktiv','FALSE','company_name5a626c6efed2d8140dd475eadae80f0f','1977-01-01','Gewerbebetrieb','2019-06-01')
,('326d8b84480a51c7e4395ca245017bed','eingestellt / ruhend','UNKNOWN','company_name326d8b84480a51c7e4395ca245017bed','1983-10-11','GmbH & Co. KG','2019-06-01')
,('471c318196e8937558e7fe76d619e802','aktiv','FALSE','company_name471c318196e8937558e7fe76d619e802','1980-12-10','GmbH','2019-06-01')
;
INSERT INTO public.company (company_id,status,rating_threshold,company_name,foundation_date,legal_form,created_at) VALUES
('7f0955e0ab83795c128530625ea173d4','aktiv','FALSE','company_name7f0955e0ab83795c128530625ea173d4','1978-08-01','GmbH','2019-06-01')
,('3018b87f8c3ef13f275333a68f49ee44','aktiv','TRUE','company_name3018b87f8c3ef13f275333a68f49ee44','1957-01-01','GmbH & Co. KG','2019-06-01')
,('a4a45f1ed06383542a2bc5a60d0a924e','aktiv','FALSE','company_namea4a45f1ed06383542a2bc5a60d0a924e','1979-01-01','Gewerbebetrieb','2019-06-01')
,('cc3bb127da80beb7408f61eb3820e951','eingestellt / ruhend','UNKNOWN','company_namecc3bb127da80beb7408f61eb3820e951',NULL,'GmbH','2019-06-01')
,('07ba0b2165c01e34b93d2a892aaddb40','aufgelöst','TRUE','company_name07ba0b2165c01e34b93d2a892aaddb40','1984-03-01','GmbH','2019-06-01')
,('d6afec8f3f57480b62f1b53521bc235c','aktiv','FALSE','company_named6afec8f3f57480b62f1b53521bc235c','1990-01-01','Gewerbebetrieb','2019-06-01')
,('b10f63419d97f20fdbff273641c04e19','aktiv','FALSE','company_nameb10f63419d97f20fdbff273641c04e19','1977-10-01','GmbH','2019-06-01')
,('f64ed7bd45f813ec0d711b3bf17512ae','aktiv','FALSE','company_namef64ed7bd45f813ec0d711b3bf17512ae','1969-01-01','Gewerbebetrieb','2019-06-01')
,('a6e22ec9ee46cfc046c06b1e3007dffe','aktiv','FALSE','company_namea6e22ec9ee46cfc046c06b1e3007dffe','1975-01-01','Gewerbebetrieb','2019-06-01')
,('1fbd1f2f50c8d379bcec8e39e41d84fe','aktiv','UNKNOWN','company_name1fbd1f2f50c8d379bcec8e39e41d84fe','1983-04-15','GmbH & Co. KG','2019-06-01')
;
INSERT INTO public.company (company_id,status,rating_threshold,company_name,foundation_date,legal_form,created_at) VALUES
('7f5dea29def9f991ee7f24156ca210e5','aktiv','FALSE','company_name7f5dea29def9f991ee7f24156ca210e5','1960-01-01','Gewerbebetrieb','2019-06-01')
,('5a7cf51858a3afea4363d9ec9c8bef3b','aktiv','FALSE','company_name5a7cf51858a3afea4363d9ec9c8bef3b','1969-12-01','OHG','2019-06-01')
,('843a94a7cef2916869fba0b23c191519','aktiv','UNKNOWN','company_name843a94a7cef2916869fba0b23c191519','1967-03-01','Einzelfirma','2019-06-01')
,('594dcfd568ec523bbd53e06047dd5b75','aktiv','FALSE','company_name594dcfd56evil8ec523bbd53e06047dd5b75','1935-01-01','GmbH','2019-06-01')
,('0826a1e96bf60d6a00f59788d1f90bc7','aktiv','FALSE','company_name0826a1e96bf60d6a00f59788d1f90bc7','1983-07-01','Gewerbebetrieb','2019-06-01')
,('29635deedbf4e75cd30297bfbe37cb68','aktiv','FALSE','company_name29635deedbf4e75cd30297bfbe37cb68','1983-07-01','Gewerbebetrieb','2019-06-01')
,('9b33fb5b804d2728c8966007a3dcb1ec','in Liquidation','TRUE','company_name9b33fb5b804d2728c8966007a3dcb1ec','1951-06-04','GmbH & Co. KG','2019-06-01')
,('4ef8a9bed1981126bad12f7eb3956e40','aktiv','FALSE','company_name4ef8a9bed1981126bad12f7eb3956e40','1978-02-20','GmbH','2019-06-01')
,('68aadd6671af61227d4504ed9eef548a','aktiv','UNKNOWN','company_name68aadd6671af61227d4504ed9eef548a','1983-03-07','Gewerbebetrieb','2019-06-01')
,('aa2500757493a9476929a0a775da3d96','in Liquidation','TRUE','company_nameaa2500757493a9476929a0a775da3d96','1872-01-01','GmbH & Co. KG','2019-06-01')
;
INSERT INTO public.company (company_id,status,rating_threshold,company_name,foundation_date,legal_form,created_at) VALUES
('d9ee2338b9d641e92b3c5da75dc3d489','aktiv','UNKNOWN','company_named9ee2338b9d641e92b3c5da75dc3d489','1982-07-01','Gewerbebetrieb','2019-06-01')
,('0b5b6d797614ba19e96f3c69c58c97f7','aktiv','TRUE','company_name0b5b6d797614ba19e96f3c69c58c97f7','1974-05-20','GmbH','2019-06-01')
,('64ee8652ed616a9623c0f5c6ca69dd45','aktiv','FALSE','company_name64ee8652ed616a9623c0f5c6ca69dd45','1959-07-02','GmbH & Co. KG','2019-06-01')
,('fedffaecb3a228c183bc9fe10e5f5d6a','eingestellt / ruhend','UNKNOWN','company_namefedffaecb3a228c183bc9fe10e5f5d6a','1948-01-01','Einzelfirma','2019-06-01')
,('caf8a36b30f664bfd716e98a6b2e978e','aktiv','FALSE','company_namecaf8a36b30f664bfd716e98a6b2e978e','1946-01-01','KG','2019-06-01')
,('7ae63f7e98937be13ec7c9215bde9737','aktiv','FALSE','company_name7ae63f7e98937be13ec7c9215bde9737','1968-01-29','Gewerbebetrieb','2019-06-01')
,('cfa8a7663ded8585889ab119fb3d9823','aktiv','TRUE','company_namecfa8a766fake3ded8585889ab119fb3d9823','1973-05-09','GmbH','2019-06-01')
,('f115e8846cf8b21fe5651fb4be8631dd','aktiv','FALSE','company_namef115e8846cf8b21fe5651fb4be8631dd','1933-08-23','KG','2019-06-01')
,('37e6aece15b2e7585b0ffce3fb2d4a1c','aktiv','FALSE','company_name37e6aece15b2e7585b0ffce3fb2d4a1c','1967-09-01','GmbH & Co. KG','2019-06-01')
,('373903b8c14dda1288ca021eede880d8','aktiv','FALSE','company_name373903b8c14dda1288ca021eede880d8','1978-03-01','Gewerbebetrieb','2019-06-01')
;
INSERT INTO public.company (company_id,status,rating_threshold,company_name,foundation_date,legal_form,created_at) VALUES
('df6d86f8772b34c43505034466a8876e','aktiv','TRUE','company_namedf6d86f8772b34c43505034466a8876e','1982-09-01','Gewerbebetrieb','2019-06-01')
,('accc612913163fc822042096c8511311','aktiv','FALSE','company_nameaccc612913163fc822042096c8511311','1969-09-01','Gewerbebetrieb','2019-06-01')
,('585f245e69e0dbb3df4e392365887cfc','aktiv','UNKNOWN','company_name585f245e69e0dbb3df4e392365887cfc','1983-01-20','Gewerbebetrieb','2019-06-01')
,('32264310ee08356d0755589a3d0e7547','aktiv','FALSE','company_name32264310ee08356d0755589a3d0e7547','1919-01-01','GmbH','2019-06-01')
,('f3b98eb6e610fb0d4b03cb0eb20916d9','aktiv','FALSE','company_namef3b98eb6e610fb0d4b03cb0eb20916d9','1979-08-13','GmbH','2019-06-01')
,('506b40d16ccac41247c9a47f0bf42dfb','aktiv','FALSE','company_name506b40d16ccac41247c9a47f0bf42dfb','1967-08-21','Einzelfirma','2019-06-01')
,('6e683892d432532f8d79cf3e99fe1cdc','aktiv','FALSE','company_name6e683892d432532f8d79cf3e99fe1cdc','1959-10-15','Gewerbebetrieb','2019-06-01')
,('682c2f8e52dec3a66f2d4932d088a83b','aktiv','FALSE','company_name682c2f8e52dec3a66f2d4932d088a83b','1977-11-08','GmbH','2019-06-01')
,('5ed85e8e6e6a98bd8465832848cb67ee','aktiv','FALSE','company_name5ed85e8e6e6a98bd8465832848cb67ee','1978-06-15','GmbH','2019-06-01')
,('5c8a9d612f73c10b1620a8db801d3873','aktiv','FALSE','company_name5c8a9d612f73c10b1620a8db801d3873','1965-07-13','GmbH','2019-06-01')
;

INSERT INTO public.company (company_id,status,rating_threshold,company_name,foundation_date,legal_form,created_at) VALUES
('cbe3afef55ae8bc74002682aa62e7e62','aktiv','FALSE','company_namecbe3afef55ae8bc74002682aa62e7e62','1981-04-03','GmbH','2019-06-01')
,('3d170269c5942f5be83554269f604c4b','aktiv','FALSE','company_name3d170269c5942f5be83554269f604c4b','1961-05-01','GmbH','2019-06-01')
,('84667c37ca4d6373f5c36200759b52f1','aktiv','FALSE','company_name84667c37ca4d6373f5c36200759b52f1','1981-10-01','Gewerbebetrieb','2019-06-01')
,('c01a1dee68d51d749e22c95c07892217','aktiv','FALSE','company_namec01a1dee68d51d749e22c95c07892217','1732-05-23','GmbH','2019-06-01')
,('56d7fbb3f3427109eab58a718e61c6aa','eingestellt / ruhend','UNKNOWN','company_name56d7fbb3f3427109eab58a718e61c6aa','1980-05-19','Gewerbebetrieb','2019-06-01')
,('a655a26861ac750a3cfbc352e6725e1a','aktiv','UNKNOWN','company_namea655a26861ac750a3cfbc352e6725e1a','1924-01-01','Einzelfirma','2019-06-01')
,('9b5e6d0170b873b599b7a30138b3698c','aktiv','UNKNOWN','company_name9b5e6d0170b873b599b7a30138b3698c','1983-09-02','GmbH','2019-06-01')
,('a457840bb70fb8faa00fd78be1e98ea4','aufgelöst','UNKNOWN','company_namea457840bb70fb8faa00fd78be1e98ea4','1981-06-05','GmbH & Co. KG','2019-06-01')
,('ed0a8c95f2ea89cfc0032bb7065b1fe5','aktiv','FALSE','company_nameed0a8c95f2ea89cfc0032bb7065b1fe5','1980-01-05','Gewerbebetrieb','2019-06-01')
;