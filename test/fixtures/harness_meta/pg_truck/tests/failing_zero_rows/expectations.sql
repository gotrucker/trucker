-- No row matches id = 9999, so this statement returns zero rows and must fail.
SELECT true FROM public.widgets_flat WHERE id = 9999;
