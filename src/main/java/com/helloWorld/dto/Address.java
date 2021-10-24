package com.helloWorld.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@JsonPropertyOrder({"doorNumber", "street", "city", "pincode"})
@JsonInclude(JsonInclude.Include.NON_NULL)
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Address {

  @JsonProperty("doorNumber")
  private String doorNumber;

  @JsonProperty("street")
  private String street;

  @JsonProperty("city")
  private String city;

  @JsonProperty("pincode")
  private String pincode;
}
