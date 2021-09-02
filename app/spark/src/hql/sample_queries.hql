-- Games status by genres
select g.genre.name, sum(status.playing) as playing_sum
from games_attributes games
lateral view explode(games.genres) g as genre
group by g.genre.name order by playing_sum desc;