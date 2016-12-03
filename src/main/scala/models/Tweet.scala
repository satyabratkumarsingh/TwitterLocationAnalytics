package models;

/**
 * Created by Satya on 27/11/2016.
 */

case class Location(latitude : BigDecimal, longitude : BigDecimal)
case class Tweet(userId: String, text: String, location : Location)

