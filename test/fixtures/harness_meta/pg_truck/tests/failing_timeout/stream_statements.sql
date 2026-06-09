-- Writes only to the non-replicated table. Postgres skips empty transactions for the
-- publication, so the truck's output LSN can never reach this WAL position and the output
-- LSN barrier must time out.
INSERT INTO public.unwatched (note) VALUES ('not replicated');
