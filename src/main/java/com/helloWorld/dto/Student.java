package com.helloWorld.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@JsonPropertyOrder({"id", "name", "address", "contact"})
@JsonInclude(JsonInclude.Include.NON_NULL)
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Student {

  @JsonProperty("id")
  private int id;

  @JsonProperty("name")
  private String name;

  @JsonProperty("address")
  private Address address;

  @JsonProperty("contact")
  private String contact;
}
