package com.codingharbour.kafka.producer;

public enum ProvinceEnum {

    ONTARIO("Ontario",	15608369L),
    QUEBEC("Quebec",	8874683L),
    NOVA_SCOTIA("Nova Scotia",	1058694L),
    NEW_BRUNSWICK("New Brunswick",	834691L),
    MANITOBA("Manitoba",	1454902L),
    BRITISH_COLUMBIA("British Columbia",	5519013L),
    PRINCE_EDWARD_ISLAND("Prince Edward Island",	173787L),
    SASKATCHEWAN("Saskatchewan",	1209107L),
    ALBERTA("Alberta",	4695290L),
    NEWFOUNDLAND_AND_LABRADOR("Newfoundland and Labrador",	538605L),
    NORTHWEST_TERRITORIES("Northwest Territories",	44972L),
    YUKON("Yukon",	44975L),
    NUNAVUT("Nunavut",	40673L);

    private String provinceName;
    private Long population;

    public String getProvinceName() {
        return provinceName;
    }

    public void setProvinceName(String provinceName) {
        this.provinceName = provinceName;
    }

    public Long getPopulation() {
        return population;
    }

    public void setPopulation(Long population) {
        this.population = population;
    }

    ProvinceEnum(String provinceName, Long population) {
        this.provinceName = provinceName;
        this.population = population;
    }


}
