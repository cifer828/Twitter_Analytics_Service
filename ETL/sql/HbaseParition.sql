SET @total := (SELECT COUNT(*) FROM all_contact_reply);
SELECT @row:=@row+1 AS row_id, uid
(SELECT uid FROM
all_contact_reply
ORDER BY uid) t
WHERE row_id in 
(@total / 10, @total / 10 * 2, @total / 10 * 3, @total / 10 * 4, @total / 10 * 5, 
@total / 10 * 6, @total / 10 * 7, @total / 10 * 8, @total / 10 * 9)


