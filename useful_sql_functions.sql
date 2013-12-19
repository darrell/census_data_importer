-- for calcluating margins of error when summing related variables
-- called as SELECT moe(ARRAY[x1,x2,...]) where x1, x2, ... are the appropriate values from the MOE tables.
-- for example, to find Total Owner Occupied Households, using table B11012 "HOUSEHOLD TYPE BY TENURE", we have to add up
-- columns, and calculate the moe:
-- b110120004 -- Owner-occupied housing units, Married-couple family
-- b110120008 -- Owner-occupied housing units, Male householder, no wife present
-- b110120011 -- Owner-occupied housing units, Female householder, no husband present
-- b110120014 -- Owner-occupied housing units, Nonfamily households
--
--SELECT "hh"."b110120008" +  "hh"."b110120011" + "hh"."b110120004" + "hh"."b110120014"  as total_owner_occupied,
--       moe( ARRAY["hh_moe"."b110120008", "hh_moe"."b110120011", "hh_moe"."b110120004", "hh_moe"."b110120014"]) as total_owner_occupied_moe
--FROM "acs2012_5yr"."geoheader" AS geo
--LEFT OUTER JOIN "acs2012_5yr"."b11012" AS hh ON ("geo"."logrecno"="hh"."logrecno"
--                                             AND "geo"."stusab"="hh"."stusab")
--LEFT OUTER JOIN "acs2012_5yr"."b11012" AS hh_moe ON ("geo"."logrecno"="hh_moe"."logrecno"
--                                                 AND "geo"."stusab"="hh_moe"."stusab")

CREATE OR REPLACE FUNCTION moe(in_moes numeric[], OUT moe integer)  as $$
  DECLARE 
  x numeric;
  tmp numeric := 0.0 ;
  BEGIN
    FOREACH x IN ARRAY in_moes 
    LOOP tmp := (x/1.645)^2;
    END LOOP;
    moe:=round(sqrt(tmp),0);
    RETURN;
  END;
$$ IMMUTABLE LANGUAGE plpgsql;
