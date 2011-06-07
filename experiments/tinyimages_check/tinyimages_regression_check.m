%% Check tiny images regression from hadoop

%% 
% First run the tinyimages pca set to load it's PCs
tinyimages_regression
%%
mats =load('~/remote/nebula/publications/tsqr/experiments/tinyimages/ti_regress_mats.tmat');
b = mats(:,1);
R = mats(:,2:end);
%%
z = R\b;

%%

norm(z-y)/norm(z)