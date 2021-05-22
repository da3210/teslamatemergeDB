#!/usr/bin/python
import psycopg2
# Addresses. ID
# charges. ID,CHARGING_PROCESSES_ID
# charging_processes. ID,CAR_ID, Addresses_ID,  geofences_ID, position_ID
# drives. ID,CAR_ID, START_Addresses_ID,  START_geofences_ID, START_position_ID, END_Addresses_ID,  END_geofences_ID, END_position_ID
# geofences. ID
# positions. ID,CAR_ID, Drives_ID
# states. ID,CAR_ID.

def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters

        # connect to the PostgreSQL server
        print('Connecting to the MASTER database...')
        conn = psycopg2.connect(
            host="127.0.0.1",
            database="teslamateMASTER",
            user="postgres",
            password="XXXXX")
		
        # create a cursor
        cur = conn.cursor()
        
        
        print('Connecting to the SECONDARY database...')
        conn2 = psycopg2.connect(
            host="127.0.0.1",
            database="teslamateSECONDARY",
            user="postgres",
            password="XXXXX")
		
        # create a cursor
        cur2 = conn2.cursor()
        
	# recover al maxid for master databases
    #Addresses
        print('MAXID Addresses:')
        cur.execute('SELECT MAX(ID) FROM public.Addresses')
        masterAddressessmax = int(''.join(map(str, cur.fetchone())))
        masterAddressessmax = masterAddressessmax+1
        print(masterAddressessmax)
    #Charges
        print('MAXID charges:')
        cur.execute('SELECT MAX(ID) FROM public.charges')
        masterchargesmax = int(''.join(map(str, cur.fetchone())))
        print(masterchargesmax)       
    #charging_processes
        print('MAXID charging_processes:')
        cur.execute('SELECT MAX(ID) FROM public.charging_processes')
        masterchargingmax = int(''.join(map(str, cur.fetchone())))
        print(masterchargingmax)  
    #drives
        print('MAXID drives:')
        cur.execute('SELECT MAX(ID) FROM public.drives')
        masterdrivesmax = int(''.join(map(str, cur.fetchone())))
        print(masterdrivesmax)  
    #geofences
        print('MAXID geofences:')
        cur.execute('SELECT MAX(ID) FROM public.geofences')
        mastergeofencesmax = int(''.join(map(str, cur.fetchone())))
        print(mastergeofencesmax)  
    #positions
        print('MAXID positions:')
        cur.execute('SELECT MAX(ID) FROM public.positions')
        masterpositionsmax = int(''.join(map(str, cur.fetchone())))
        print(masterpositionsmax)  
    #states
        print('MAXID states:')
        cur.execute('SELECT MAX(ID) FROM public.states')
        masterstatesmax = int(''.join(map(str, cur.fetchone())))
        print(masterstatesmax) 


    # Remove all constrains in SECONDARY DB
        print('SECONDARY addresses pkey  delete')
        cur2.execute('ALTER TABLE public.addresses DROP CONSTRAINT IF EXISTS addresses_pkey CASCADE;')
        conn2.commit()
        print('SECONDARY charges pkey delete')
        cur2.execute('ALTER TABLE public.charges DROP CONSTRAINT IF EXISTS charges_pkey CASCADE;')
        conn2.commit()
        print('SECONDARY charging_processes pkey  delete')
        cur2.execute('ALTER TABLE public.charging_processes DROP CONSTRAINT IF EXISTS charging_processes_pkey CASCADE;')
        conn2.commit()
        print('SECONDARY drives pkey  delete')
        cur2.execute('ALTER TABLE public.drives DROP CONSTRAINT IF EXISTS trips_pkey CASCADE;')
        conn2.commit()
        print('SECONDARY geofences pkey  delete')
        cur2.execute('ALTER TABLE public.geofences DROP CONSTRAINT IF EXISTS geofences_pkey CASCADE;')
        conn2.commit()
        print('SECONDARY positions pkey  delete')
        cur2.execute('ALTER TABLE public.positions DROP CONSTRAINT IF EXISTS positions_pkey CASCADE;')
        conn2.commit()
        print('SECONDARY states pkey  delete')
        cur2.execute('ALTER TABLE public.states DROP CONSTRAINT IF EXISTS states_pkey CASCADE;')
        conn2.commit()


    #update pkey values for all tables
        print('Updating all pkey in Secondary DB...')
        print('SECONDARY addresses increment ID')
        cur2.execute('UPDATE addresses SET id = id + '+str(masterAddressessmax))
        conn2.commit()
        print('SECONDARY charges increment ID')
        cur2.execute('UPDATE charges SET id = id + '+str(masterchargesmax))
        conn2.commit()
        print('SECONDARY charging_processes increment ID')
        cur2.execute('UPDATE charging_processes SET id = id + '+str(masterchargingmax))
        conn2.commit()
        print('SECONDARY drives increment ID')
        cur2.execute('UPDATE drives SET id = id + '+str(masterdrivesmax))
        conn2.commit()
        print('SECONDARY geofences increment ID')
        cur2.execute('UPDATE geofences SET id = id + '+str(mastergeofencesmax))
        conn2.commit()
        print('SECONDARY positions increment ID')
        cur2.execute('UPDATE positions SET id = id + '+str(masterpositionsmax))
        conn2.commit()
        print('SECONDARY states increment ID')
        cur2.execute('UPDATE states SET id = id + '+str(masterstatesmax))
        conn2.commit()

        print('Updating references to other tables...')
        print('SECONDARY Table charges increment charging_process_id')
        cur2.execute('UPDATE charges SET charging_process_id = charging_process_id + '+str(masterchargingmax))
        conn2.commit()
        print('SECONDARY TABLE CHRGING_PROCESSES')
        print('SECONDARY Table charging_processes increment address_id')
        cur2.execute('UPDATE charging_processes SET address_id = address_id + '+str(masterAddressessmax))
        conn2.commit()
        print('SECONDARY Table charging_processes increment geofence_id')
        cur2.execute('UPDATE charging_processes SET geofence_id = geofence_id + '+str(mastergeofencesmax))
        conn2.commit()
        print('SECONDARY Table charging_processes increment position_id')
        cur2.execute('UPDATE charging_processes SET position_id = position_id + '+str(masterpositionsmax))
        conn2.commit()
        #drives. ID,CAR_ID, START_Addresses_ID,  START_geofences_ID, START_position_ID, 
        #END_Addresses_ID,  END_geofences_ID, END_position_ID
        print('SECONDARY TABLE DRIVES')
        print('SECONDARY Table drives increment start_Addresses_ID')
        cur2.execute('UPDATE drives SET start_Address_ID = start_Address_ID + '+str(masterAddressessmax))
        conn2.commit()
        print('SECONDARY Table drives increment end_Addresses_ID')
        cur2.execute('UPDATE drives SET end_Address_ID = end_Address_ID + '+str(masterAddressessmax))
        conn2.commit()
        print('SECONDARY Table drives increment start_geofence_id')
        cur2.execute('UPDATE drives SET start_geofence_id = start_geofence_id + '+str(mastergeofencesmax))
        conn2.commit()
        print('SECONDARY Table drives increment end_geofence_id')
        cur2.execute('UPDATE drives SET end_geofence_id = end_geofence_id + '+str(mastergeofencesmax))
        conn2.commit()
        print('SECONDARY Table drives increment start_position_id')
        cur2.execute('UPDATE drives SET start_position_id = start_position_id + '+str(masterpositionsmax))
        conn2.commit()
        print('SECONDARY Table drives increment end_position_id')
        cur2.execute('UPDATE drives SET end_position_id = end_position_id + '+str(masterpositionsmax))
        conn2.commit()
        print('SECONDARY TABLE POSITIONS')
        print('SECONDARY Table positions increment drives')
        cur2.execute('UPDATE positions SET drive_id = drive_id + '+str(masterdrivesmax))
        conn2.commit()

        print('CREATE PKEY in SECONDARY TO CHECK')

        print('SECONDARY addresses pkey create')
        cur2.execute('ALTER TABLE public.addresses ADD CONSTRAINT addresses_pkey PRIMARY KEY (id);')
        conn2.commit()
        print('SECONDARY charges pkey create')
        cur2.execute('ALTER TABLE public.charges ADD CONSTRAINT charges_pkey PRIMARY KEY (id);')
        conn2.commit()
        print('SECONDARY charging_processes pkey create')
        cur2.execute('ALTER TABLE public.charging_processes ADD CONSTRAINT charging_processes_pkey PRIMARY KEY (id);')
        conn2.commit()
        print('SECONDARY drives pkey create')
        cur2.execute('ALTER TABLE public.drives ADD CONSTRAINT trips_pkey PRIMARY KEY (id);')
        conn2.commit()
        print('SECONDARY charges pkey create')
        cur2.execute('ALTER TABLE public.geofences ADD CONSTRAINT geofences_pkey PRIMARY KEY (id);')
        conn2.commit()
        print('SECONDARY charges pkey create')
        cur2.execute('ALTER TABLE public.positions ADD CONSTRAINT positions_pkey PRIMARY KEY (id);')
        conn2.commit()
        print('SECONDARY charges pkey create')
        cur2.execute('ALTER TABLE public.states ADD CONSTRAINT states_pkey PRIMARY KEY (id);')
        conn2.commit()

        print('CREATE constrains for references IDs')

        print('SECONDARY Table charges contraint to charging_processes')
        cur2.execute('ALTER TABLE public.charges ADD CONSTRAINT charges_charging_process_id_fkey FOREIGN KEY (charging_process_id)     REFERENCES public.charging_processes (id) MATCH SIMPLE     ON UPDATE NO ACTION     ON DELETE CASCADE;')
        conn2.commit()
        print('SECONDARY Table charging_processes contraint to adresses')
        cur2.execute('ALTER TABLE public.charging_processes     ADD CONSTRAINT charging_processes_address_id_fkey FOREIGN KEY (address_id)    REFERENCES public.addresses (id) MATCH SIMPLE    ON UPDATE NO ACTION    ON DELETE SET NULL;')
        conn2.commit()
        print('SECONDARY Table charging_processes contraint to geofences')
        cur2.execute('ALTER TABLE public.charging_processes     ADD CONSTRAINT charging_processes_geofence_id_fkey FOREIGN KEY (geofence_id)    REFERENCES public.geofences (id) MATCH SIMPLE    ON UPDATE NO ACTION    ON DELETE SET NULL;')
        conn2.commit()
        print('SECONDARY Table charging_processes contraint to positions')
        cur2.execute('ALTER TABLE public.charging_processes    ADD CONSTRAINT charging_processes_position_id_fkey FOREIGN KEY (position_id)    REFERENCES public.positions (id) MATCH SIMPLE    ON UPDATE NO ACTION    ON DELETE NO ACTION;')
        conn2.commit()
        print('SECONDARY Table drive contraint to start_address')
        cur2.execute('ALTER TABLE public.drives    ADD CONSTRAINT drives_start_address_id_fkey FOREIGN KEY (start_address_id)    REFERENCES public.addresses (id) MATCH SIMPLE    ON UPDATE NO ACTION   ON DELETE SET NULL;')
        conn2.commit()
        print('SECONDARY Table drive contraint to end_address')
        cur2.execute('ALTER TABLE public.drives    ADD CONSTRAINT drives_end_address_id_fkey FOREIGN KEY (end_address_id)    REFERENCES public.addresses (id) MATCH SIMPLE   ON UPDATE NO ACTION    ON DELETE SET NULL;')
        conn2.commit()
        print('SECONDARY Table drive contraint to start_geofence')
        cur2.execute('ALTER TABLE public.drives    ADD CONSTRAINT drives_start_geofence_id_fkey FOREIGN KEY (start_geofence_id)    REFERENCES public.geofences (id) MATCH SIMPLE    ON UPDATE NO ACTION    ON DELETE SET NULL;')
        conn2.commit()
        print('SECONDARY Table drive contraint to end_geofence')
        cur2.execute('ALTER TABLE public.drives    ADD CONSTRAINT drives_end_geofence_id_fkey FOREIGN KEY (end_geofence_id)    REFERENCES public.geofences (id) MATCH SIMPLE    ON UPDATE NO ACTION    ON DELETE SET NULL;')
        conn2.commit()
        print('SECONDARY Table drive contraint to start_position')
        cur2.execute('ALTER TABLE public.drives    ADD CONSTRAINT drives_start_position_id_fkey FOREIGN KEY (start_position_id)    REFERENCES public.positions (id) MATCH SIMPLE    ON UPDATE NO ACTION    ON DELETE SET NULL;')
        conn2.commit()
        print('SECONDARY Table drive contraint to end_position')
        cur2.execute('ALTER TABLE public.drives    ADD CONSTRAINT drives_end_position_id_fkey FOREIGN KEY (end_position_id)    REFERENCES public.positions (id) MATCH SIMPLE    ON UPDATE NO ACTION    ON DELETE SET NULL;')
        conn2.commit()
        print('SECONDARY Table position contraint to drives')
        cur2.execute('ALTER TABLE public.positions    ADD CONSTRAINT positions_drive_id_fkey FOREIGN KEY (drive_id)    REFERENCES public.drives (id) MATCH SIMPLE    ON UPDATE NO ACTION    ON DELETE SET NULL;')
        conn2.commit()

# DELETE REFERENCES IN BOTH DB
        print('SECONDARY addresses pkey  delete')
        cur2.execute('ALTER TABLE public.addresses DROP CONSTRAINT IF EXISTS addresses_pkey CASCADE;')
        conn2.commit()
        print('SECONDARY charges pkey delete')
        cur2.execute('ALTER TABLE public.charges DROP CONSTRAINT IF EXISTS charges_pkey CASCADE;')
        conn2.commit()
        print('SECONDARY charging_processes pkey  delete')
        cur2.execute('ALTER TABLE public.charging_processes DROP CONSTRAINT IF EXISTS charging_processes_pkey CASCADE;')
        conn2.commit()
        print('SECONDARY drives pkey  delete')
        cur2.execute('ALTER TABLE public.drives DROP CONSTRAINT IF EXISTS trips_pkey CASCADE;')
        conn2.commit()
        print('SECONDARY geofences pkey  delete')
        cur2.execute('ALTER TABLE public.geofences DROP CONSTRAINT IF EXISTS geofences_pkey CASCADE;')
        conn2.commit()
        print('SECONDARY positions pkey  delete')
        cur2.execute('ALTER TABLE public.positions DROP CONSTRAINT IF EXISTS positions_pkey CASCADE;')
        conn2.commit()
        print('SECONDARY states pkey  delete')
        cur2.execute('ALTER TABLE public.states DROP CONSTRAINT IF EXISTS states_pkey CASCADE;')
        conn2.commit()
        print('MASTER addresses pkey  delete')
        cur.execute('ALTER TABLE public.addresses DROP CONSTRAINT IF EXISTS addresses_pkey CASCADE;')
        conn.commit()
        print('MASTER charges pkey delete')
        cur.execute('ALTER TABLE public.charges DROP CONSTRAINT IF EXISTS charges_pkey CASCADE;')
        conn.commit()
        print('MASTER charging_processes pkey  delete')
        cur.execute('ALTER TABLE public.charging_processes DROP CONSTRAINT IF EXISTS charging_processes_pkey CASCADE;')
        conn.commit()
        print('MASTER drives pkey  delete')
        cur.execute('ALTER TABLE public.drives DROP CONSTRAINT IF EXISTS trips_pkey CASCADE;')
        conn.commit()
        print('MASTER geofences pkey  delete')
        cur.execute('ALTER TABLE public.geofences DROP CONSTRAINT IF EXISTS geofences_pkey CASCADE;')
        conn.commit()
        print('MASTER positions pkey  delete')
        cur.execute('ALTER TABLE public.positions DROP CONSTRAINT IF EXISTS positions_pkey CASCADE;')
        conn.commit()
        print('MASTER states pkey  delete')
        cur.execute('ALTER TABLE public.states DROP CONSTRAINT IF EXISTS states_pkey CASCADE;')
        conn.commit()

# DROP INDEXES IN MASTER
        print('DROPING INDEXES in master')
        cur.execute('DROP INDEX public.addresses_osm_id_osm_type_index;')
        conn.commit()
        print('DROPING INDEXES in master')
        cur.execute('DROP INDEX public."states_car_id__end_date_IS_NULL_index";')
        conn.commit()
        
# COPY DATA FROM DB SECONDARY TO PRIMARY
        print('COPYING ADDRESSES DATA')
        cur2.execute('COPY addresses TO \'C:/temp/addresses.CSV\' CSV ')
        conn2.commit()
        print('COPYING ADDRESSES DATA')
        cur.execute('COPY addresses FROM \'C:/temp/addresses.CSV\' CSV ')
        conn.commit()
        print('COPYING charges DATA')
        cur2.execute('COPY charges TO \'C:/temp/charges.CSV\' CSV ')
        conn2.commit()
        cur.execute('COPY charges FROM \'C:/temp/charges.CSV\' CSV ')
        conn.commit()

        print('COPYING charging_processes DATA')
        cur2.execute('COPY charging_processes TO \'C:/temp/charging.CSV\' CSV ')
        conn2.commit()
        cur.execute('COPY charging_processes FROM \'C:/temp/charging.CSV\' CSV ')
        conn.commit()

        print('COPYING drives DATA')
        cur2.execute('COPY drives TO \'C:/temp/drives.CSV\' CSV ')
        conn2.commit()
        cur.execute('COPY drives FROM \'C:/temp/drives.CSV\' CSV ')
        conn.commit()

        print('COPYING geofences DATA')
        cur2.execute('COPY geofences TO \'C:/temp/geofences.CSV\' CSV ')
        conn2.commit()
        cur.execute('COPY geofences FROM \'C:/temp/geofences.CSV\' CSV ')
        conn.commit()

        print('COPYING positions DATA')
        cur2.execute('COPY positions TO \'C:/temp/positions.CSV\' CSV ')
        conn2.commit()
        cur.execute('COPY positions FROM \'C:/temp/positions.CSV\' CSV ')
        conn.commit()

        print('COPYING states DATA')
        cur2.execute('COPY states TO \'C:/temp/states.CSV\' CSV ')
        conn2.commit()
        cur.execute('COPY states FROM \'C:/temp/states.CSV\' CSV ')
        conn.commit()                        


# CREATE PKEY in PRIMARY
        print('MASTER addresses pkey create')
        cur.execute('ALTER TABLE public.addresses ADD CONSTRAINT addresses_pkey PRIMARY KEY (id);')
        conn.commit()
        print('MASTER charges pkey create')
        cur.execute('ALTER TABLE public.charges ADD CONSTRAINT charges_pkey PRIMARY KEY (id);')
        conn.commit()
        print('MASTER charging_processes pkey create')
        cur.execute('ALTER TABLE public.charging_processes ADD CONSTRAINT charging_processes_pkey PRIMARY KEY (id);')
        conn.commit()
        print('MASTER drives pkey create')
        cur.execute('ALTER TABLE public.drives ADD CONSTRAINT trips_pkey PRIMARY KEY (id);')
        conn.commit()
        print('MASTER charges pkey create')
        cur.execute('ALTER TABLE public.geofences ADD CONSTRAINT geofences_pkey PRIMARY KEY (id);')
        conn.commit()
        print('MASTER charges pkey create')
        cur.execute('ALTER TABLE public.positions ADD CONSTRAINT positions_pkey PRIMARY KEY (id);')
        conn.commit()
        print('MASTER charges pkey create')
        cur.execute('ALTER TABLE public.states ADD CONSTRAINT states_pkey PRIMARY KEY (id);')
        conn.commit()

        print('CREATE constrains for references IDs')

        print('MASTER Table charges contraint to charging_processes')
        cur.execute('ALTER TABLE public.charges ADD CONSTRAINT charges_charging_process_id_fkey FOREIGN KEY (charging_process_id)     REFERENCES public.charging_processes (id) MATCH SIMPLE     ON UPDATE NO ACTION     ON DELETE CASCADE;')
        conn.commit()
        print('MASTER Table charging_processes contraint to adresses')
        cur.execute('ALTER TABLE public.charging_processes     ADD CONSTRAINT charging_processes_address_id_fkey FOREIGN KEY (address_id)    REFERENCES public.addresses (id) MATCH SIMPLE    ON UPDATE NO ACTION    ON DELETE SET NULL;')
        conn.commit()
        print('MASTER Table charging_processes contraint to geofences')
        cur.execute('ALTER TABLE public.charging_processes     ADD CONSTRAINT charging_processes_geofence_id_fkey FOREIGN KEY (geofence_id)    REFERENCES public.geofences (id) MATCH SIMPLE    ON UPDATE NO ACTION    ON DELETE SET NULL;')
        conn.commit()
        print('MASTER Table charging_processes contraint to positions')
        cur.execute('ALTER TABLE public.charging_processes    ADD CONSTRAINT charging_processes_position_id_fkey FOREIGN KEY (position_id)    REFERENCES public.positions (id) MATCH SIMPLE    ON UPDATE NO ACTION    ON DELETE NO ACTION;')
        conn.commit()
        print('MASTER Table drive contraint to start_address')
        cur.execute('ALTER TABLE public.drives    ADD CONSTRAINT drives_start_address_id_fkey FOREIGN KEY (start_address_id)    REFERENCES public.addresses (id) MATCH SIMPLE    ON UPDATE NO ACTION   ON DELETE SET NULL;')
        conn.commit()
        print('MASTER Table drive contraint to end_address')
        cur.execute('ALTER TABLE public.drives    ADD CONSTRAINT drives_end_address_id_fkey FOREIGN KEY (end_address_id)    REFERENCES public.addresses (id) MATCH SIMPLE   ON UPDATE NO ACTION    ON DELETE SET NULL;')
        conn.commit()
        print('MASTER Table drive contraint to start_geofence')
        cur.execute('ALTER TABLE public.drives    ADD CONSTRAINT drives_start_geofence_id_fkey FOREIGN KEY (start_geofence_id)    REFERENCES public.geofences (id) MATCH SIMPLE    ON UPDATE NO ACTION    ON DELETE SET NULL;')
        conn.commit()
        print('MASTER Table drive contraint to end_geofence')
        cur.execute('ALTER TABLE public.drives    ADD CONSTRAINT drives_end_geofence_id_fkey FOREIGN KEY (end_geofence_id)    REFERENCES public.geofences (id) MATCH SIMPLE    ON UPDATE NO ACTION    ON DELETE SET NULL;')
        conn.commit()
        print('MASTER Table drive contraint to start_position')
        cur.execute('ALTER TABLE public.drives    ADD CONSTRAINT drives_start_position_id_fkey FOREIGN KEY (start_position_id)    REFERENCES public.positions (id) MATCH SIMPLE    ON UPDATE NO ACTION    ON DELETE SET NULL;')
        conn.commit()
        print('MASTER Table drive contraint to end_position')
        cur.execute('ALTER TABLE public.drives    ADD CONSTRAINT drives_end_position_id_fkey FOREIGN KEY (end_position_id)    REFERENCES public.positions (id) MATCH SIMPLE    ON UPDATE NO ACTION    ON DELETE SET NULL;')
        conn.commit()
        print('MASTER Table position contraint to drives')
        cur.execute('ALTER TABLE public.positions    ADD CONSTRAINT positions_drive_id_fkey FOREIGN KEY (drive_id)    REFERENCES public.drives (id) MATCH SIMPLE    ON UPDATE NO ACTION    ON DELETE SET NULL;')
        conn.commit()

# REBUILD INDEXES
# DROP INDEXES IN MASTER
        # print('CREATING INDEXES in master')
        # cur.execute('CREATE UNIQUE INDEX addresses_osm_id_osm_type_index    ON public.addresses USING btree    (osm_id ASC NULLS LAST, osm_type COLLATE pg_catalog."default" ASC NULLS LAST)    TABLESPACE pg_default;')
        # conn.commit()
        # cur.execute('CREATE UNIQUE INDEX "states_car_id__end_date_IS_NULL_index"    ON public.states USING btree    (car_id ASC NULLS LAST, (end_date IS NULL) ASC NULLS LAST)    TABLESPACE pg_default    WHERE end_date IS NULL;')
        # conn.commit()

	# close the communication with the PostgreSQL
        cur.close()
        cur2.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database master connection closed.')
        if conn2 is not None:
            conn2.close()
            print('Database secondary connection closed.')


if __name__ == '__main__':
    connect()