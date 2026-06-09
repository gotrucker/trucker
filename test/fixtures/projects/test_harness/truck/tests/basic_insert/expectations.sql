SELECT (COUNT(*) = 3)::int FROM public.widgets_flat;
SELECT (COUNT(*) = 1)::int FROM public.widgets_flat WHERE id = 1 AND deleted = true;
SELECT (COUNT(*) = 1)::int FROM public.widgets_flat WHERE id = 2 AND name = 'seed two updated' AND deleted = false;
SELECT (COUNT(*) = 1)::int FROM public.widgets_flat WHERE id = 3 AND name = 'stream three' AND deleted = false;
