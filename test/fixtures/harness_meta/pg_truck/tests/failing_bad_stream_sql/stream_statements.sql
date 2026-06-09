-- Deliberate syntax error (missing closing paren) to exercise the stream phase failure.
INSERT INTO public.widgets (id, name VALUES (3, 'stream three');
