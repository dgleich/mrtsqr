function greygrid(ax,color)
% GREYGRID Enable grey (gray) gridlines in a plot.
% This function copies the current axes, so you should run it last before 
% finishing a plot.
if ~exist('ax','var') || isempty(ax), ax = gca; end
if ~exist('color','var') || isempty(color), color=[0.8 0.8 0.8], end;
xcol = get(ax,'XColor');
ycol = get(ax,'YColor');
grid on;
set(ax,'XColor',color);
set(ax,'YColor',color);

Caxes = copyobj(ax,gcf);
set(Caxes, 'color', 'none', 'xcolor', xcol, 'ycolor', ycol, ...
    'xgrid' ,'off','ygrid', 'off');

