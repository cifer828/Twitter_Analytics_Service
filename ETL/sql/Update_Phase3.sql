-- show global variables like 'query_cache%';
-- show global variables like 'max_connection%';

-- create index contact_uid_idx on python_combine_all(contact_uid);

-- add space to the end
SELECT contact_uid, latest_tweet, LENGTH(latest_tweet) from python_combine_all 
WHERE (uid = 2246204187 and (contact_uid = 794622552576589824 OR contact_uid = 4295818761 OR contact_uid = 928218080))
OR (uid = 2246206818 and contact_uid = 292676401)
OR (uid = 2246212798 and contact_uid = 753764056494927872)
OR (uid = 2246223644 and contact_uid = 2883512687)
OR (uid = 2246227878 and contact_uid = 16902662)
OR (uid = 156053175 and contact_uid = 2764352584)
OR (uid = 156054222 and (contact_uid = 1295588864 OR contact_uid = 323446256 OR contact_uid = 256078735 OR contact_uid = 159033916 OR contact_uid = 156476638))
OR (uid = 1560557635 and contact_uid = 2219623466)
OR (uid = 1560562164 and contact_uid = 742042865501667328)
OR (uid = 1560543786 and (contact_uid = 4905852879 OR contact_uid = 834267386 OR contact_uid = 4512568648 OR contact_uid = 4307697922 OR contact_uid = 1489580946 OR contact_uid = 1489580946))
OR (uid = 156056051 and contact_uid = 29758446)
OR (uid = 1560562164 and contact_uid = 742042865501667328)
OR (uid = 224622293 and contact_uid = 2189771672)
OR (uid = 2246248651 and contact_uid = 706430492);

UPDATE python_combine_all
SET latest_tweet = CONCAT(latest_tweet,' ')
WHERE (uid = 2246204187 and (contact_uid = 794622552576589824 OR contact_uid = 4295818761 OR contact_uid = 928218080))
OR (uid = 2246206818 and contact_uid = 292676401)
OR (uid = 2246212798 and contact_uid = 753764056494927872)
OR (uid = 2246223644 and contact_uid = 2883512687)
OR (uid = 2246227878 and contact_uid = 16902662)
OR (uid = 156053175 and contact_uid = 2764352584)
OR (uid = 156054222 and (contact_uid = 1295588864 OR contact_uid = 323446256 OR contact_uid = 256078735 OR contact_uid = 159033916 OR contact_uid = 156476638))
OR (uid = 1560557635 and contact_uid = 2219623466)
OR (uid = 1560562164 and contact_uid = 742042865501667328)
OR (uid = 1560543786 and (contact_uid = 4905852879 OR contact_uid = 834267386 OR contact_uid = 4512568648 OR contact_uid = 4307697922 OR contact_uid = 1489580946 OR contact_uid = 1489580946))
OR (uid = 156056051 and contact_uid = 29758446)
OR (uid = 1560562164 and contact_uid = 742042865501667328)
OR (uid = 224622293 and contact_uid = 2189771672)
OR (uid = 2246248651 and contact_uid = 706430492);

SELECT contact_uid, latest_tweet, LENGTH(latest_tweet) from python_combine_all 
WHERE (uid = 2246204187 and (contact_uid = 794622552576589824 OR contact_uid = 4295818761 OR contact_uid = 928218080))
OR (uid = 2246206818 and contact_uid = 292676401)
OR (uid = 2246212798 and contact_uid = 753764056494927872)
OR (uid = 2246223644 and contact_uid = 2883512687)
OR (uid = 2246227878 and contact_uid = 16902662)
OR (uid = 156053175 and contact_uid = 2764352584)
OR (uid = 156054222 and (contact_uid = 1295588864 OR contact_uid = 323446256 OR contact_uid = 256078735 OR contact_uid = 159033916 OR contact_uid = 156476638))
OR (uid = 1560557635 and contact_uid = 2219623466)
OR (uid = 1560562164 and contact_uid = 742042865501667328)
OR (uid = 1560543786 and (contact_uid = 4905852879 OR contact_uid = 834267386 OR contact_uid = 4512568648 OR contact_uid = 4307697922 OR contact_uid = 1489580946 OR contact_uid = 1489580946))
OR (uid = 156056051 and contact_uid = 29758446)
OR (uid = 1560562164 and contact_uid = 742042865501667328)
OR (uid = 224622293 and contact_uid = 2189771672)
OR (uid = 2246248651 and contact_uid = 706430492);
