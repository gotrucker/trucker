-- The output really has 3 rows; asserting 99 must fail with a non-truthy cell.
SELECT (COUNT(*) = 99)::int FROM public.widgets_flat;
