
USE jyothi;

CREATE TABLE samples(id text PRIMARY KEY, sample_number int);

CREATE TABLE users(id text PRIMARY KEY, sdk_version float, app_bundle_id text, app_name text, app_version text, app_mode text, device_uuid text, custom_user_id text, hardware_model text, os_version text, device_height int, device_width int, countryId text, langId text, stateId text);