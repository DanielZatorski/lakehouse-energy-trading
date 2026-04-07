PSR_LABELS = {
    "B01": "biomass",
    "B04": "fossil_gas",
    "B05": "hard_coal",
    "B09": "geothermal",
    "B10": "hydro_pumped_storage",
    "B11": "hydro_run_of_river",
    "B12": "hydro_reservoir",
    "B14": "nuclear",
    "B15": "other_renewable",
    "B16": "solar",
    "B17": "waste",
    "B18": "wind_offshore",
    "B19": "wind_onshore",
    "B20": "other",
}

BUSINESS_TYPE_LABELS = {
    "A01": "production",
    "A04": "consumption",
    "A93": "wind_generation",
    "A94": "solar_generation",
}


# =========================
# COUNTRY / AREA PARAMS
# =========================

entsoe_country_params = [
    {
        "country_code": "DE",
        "country": "Germany",
        "entsoe_areas": [
            {
                "area_code": "10Y1001A1001A82H",
                "area_name": "DE-LU",
                "area_type": "BZN",
                "use_for": [
                    "actual_total_load",
                    "day_ahead_total_load_forecast",
                    "day_ahead_energy_prices",
                    "day_ahead_aggregated_generation",
                    "generation_forecast_wind_solar",
                    "actual_generation_per_production_type",
                ],
            }
        ],
    },
    {
        "country_code": "PL",
        "country": "Poland",
        "entsoe_areas": [
            {
                "area_code": "10YPL-AREA-----S",
                "area_name": "PL",
                "area_type": "BZN",
                "use_for": ["all_main_market_datasets"],
            }
        ],
    },
    {
        "country_code": "DK",
        "country": "Denmark",
        "entsoe_areas": [
            {
                "area_code": "10YDK-1--------W",
                "area_name": "DK1",
                "area_type": "BZN",
                "use_for": ["all_main_market_datasets"],
            },
            {
                "area_code": "10YDK-2--------M",
                "area_name": "DK2",
                "area_type": "BZN",
                "use_for": ["all_main_market_datasets"],
            },
        ],
    },
    {
        "country_code": "SE",
        "country": "Sweden",
        "entsoe_areas": [
            {"area_code": "10Y1001A1001A44P", "area_name": "SE1", "area_type": "BZN", "use_for": ["all_main_market_datasets"]},
            {"area_code": "10Y1001A1001A45N", "area_name": "SE2", "area_type": "BZN", "use_for": ["all_main_market_datasets"]},
            {"area_code": "10Y1001A1001A46L", "area_name": "SE3", "area_type": "BZN", "use_for": ["all_main_market_datasets"]},
            {"area_code": "10Y1001A1001A47J", "area_name": "SE4", "area_type": "BZN", "use_for": ["all_main_market_datasets"]},
        ],
    },
    {
        "country_code": "ES",
        "country": "Spain",
        "entsoe_areas": [
            {"area_code": "10YES-REE------0", "area_name": "ES", "area_type": "BZN", "use_for": ["all_main_market_datasets"]}
        ],
    },
    {
        "country_code": "FR",
        "country": "France",
        "entsoe_areas": [
            {"area_code": "10YFR-RTE------C", "area_name": "FR", "area_type": "BZN", "use_for": ["all_main_market_datasets"]}
        ],
    },
    {
        "country_code": "NL",
        "country": "Netherlands",
        "entsoe_areas": [
            {"area_code": "10YNL----------L", "area_name": "NL", "area_type": "BZN", "use_for": ["all_main_market_datasets"]}
        ],
    },
    {
        "country_code": "IT",
        "country": "Italy",
        "entsoe_areas": [
            {"area_code": "10Y1001A1001A73I", "area_name": "IT-North", "area_type": "BZN", "use_for": ["all_main_market_datasets"]},
            {"area_code": "10Y1001A1001A70O", "area_name": "IT-Centre-North", "area_type": "BZN", "use_for": ["all_main_market_datasets"]},
            {"area_code": "10Y1001A1001A71M", "area_name": "IT-Centre-South", "area_type": "BZN", "use_for": ["all_main_market_datasets"]},
            {"area_code": "10Y1001A1001A788", "area_name": "IT-South", "area_type": "BZN", "use_for": ["all_main_market_datasets"]},
            {"area_code": "10Y1001A1001A75E", "area_name": "IT-Sicily", "area_type": "BZN", "use_for": ["all_main_market_datasets"]},
            {"area_code": "10Y1001A1001A74G", "area_name": "IT-Sardinia", "area_type": "BZN", "use_for": ["all_main_market_datasets"]},
        ],
    },
    {
        "country_code": "BE",
        "country": "Belgium",
        "entsoe_areas": [
            {"area_code": "10YBE----------2", "area_name": "BE", "area_type": "BZN", "use_for": ["all_main_market_datasets"]}
        ],
    },
    {
        "country_code": "AT",
        "country": "Austria",
        "entsoe_areas": [
            {"area_code": "10YAT-APG------L", "area_name": "AT", "area_type": "BZN", "use_for": ["all_main_market_datasets"]}
        ],
    },
    {
        "country_code": "CZ",
        "country": "Czechia",
        "entsoe_areas": [
            {"area_code": "10YCZ-CEPS-----N", "area_name": "CZ", "area_type": "BZN", "use_for": ["all_main_market_datasets"]}
        ],
    },
    {
        "country_code": "PT",
        "country": "Portugal",
        "entsoe_areas": [
            {"area_code": "10YPT-REN------W", "area_name": "PT", "area_type": "BZN", "use_for": ["all_main_market_datasets"]}
        ],
    },
    {
        "country_code": "RO",
        "country": "Romania",
        "entsoe_areas": [
            {"area_code": "10YRO-TEL------P", "area_name": "RO", "area_type": "BZN", "use_for": ["all_main_market_datasets"]}
        ],
    },
    {
        "country_code": "GR",
        "country": "Greece",
        "entsoe_areas": [
            {"area_code": "10YGR-HTSO-----Y", "area_name": "GR", "area_type": "BZN", "use_for": ["all_main_market_datasets"]}
        ],
    },
]



# =========================
# DATASET MAPPINGS
# =========================

DATASET_CONFIGS = {
    "actual_total_load": {
        "document_type": "A65",
        "process_type": "A16",
        "domain_mode": "out_bzn",
    },
    "day_ahead_total_load_forecast": {
        "document_type": "A65",
        "process_type": "A01",
        "domain_mode": "out_bzn",
    },
    "day_ahead_energy_prices": {
        "document_type": "A44",
        "process_type": None,
        "domain_mode": "in_out_same",
    },
    "day_ahead_aggregated_generation": {
        "document_type": "A71",
        "process_type": "A01",
        "domain_mode": "in_domain",
    },
    "generation_forecast_wind_solar": {
        "document_type": "A69",
        "process_type": None,
        "domain_mode": "in_domain",
    },
    "actual_generation_per_production_type": {
        "document_type": "A75",
        "process_type": "A16",
        "domain_mode": "in_domain",
    },
}
